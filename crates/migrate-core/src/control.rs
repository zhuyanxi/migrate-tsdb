//! Cooperative job control: start / pause / resume / stop.
//!
//! Pause and stop are honored at migration-unit boundaries (after a checkpoint
//! has been written), which bounds the worst-case latency to one unit and keeps
//! the resume semantics exactly-once at part granularity.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Created,
    Running,
    Pausing,
    Paused,
    Stopping,
    Stopped,
    Completed,
    Failed,
}

pub fn job_state_str(s: JobState) -> &'static str {
    match s {
        JobState::Created => "created",
        JobState::Running => "running",
        JobState::Pausing => "pausing",
        JobState::Paused => "paused",
        JobState::Stopping => "stopping",
        JobState::Stopped => "stopped",
        JobState::Completed => "completed",
        JobState::Failed => "failed",
    }
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        matches!(self, JobState::Completed | JobState::Failed)
    }

    pub fn is_active(self) -> bool {
        matches!(
            self,
            JobState::Running | JobState::Pausing | JobState::Paused | JobState::Stopping
        )
    }
}

struct Inner {
    state: JobState,
    active_blocks: usize,
    paused_blocks: usize,
    error: Option<String>,
}

pub struct Control {
    inner: Mutex<Inner>,
    cv: Condvar,
    pause: AtomicBool,
    stop: AtomicBool,
}

impl Default for Control {
    fn default() -> Self {
        Self::new()
    }
}

impl Control {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                state: JobState::Created,
                active_blocks: 0,
                paused_blocks: 0,
                error: None,
            }),
            cv: Condvar::new(),
            pause: AtomicBool::new(false),
            stop: AtomicBool::new(false),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn state(&self) -> JobState {
        self.lock().state
    }

    pub fn error(&self) -> Option<String> {
        self.lock().error.clone()
    }

    /// Marks the job as running. Called by the engine right before spawning
    /// workers; also used to resume a stopped job.
    pub fn start(&self) -> bool {
        let mut inner = self.lock();
        if !matches!(
            inner.state,
            JobState::Created | JobState::Stopped | JobState::Paused | JobState::Pausing
        ) {
            return false;
        }
        self.stop.store(false, Ordering::SeqCst);
        self.pause.store(false, Ordering::SeqCst);
        inner.paused_blocks = 0;
        inner.state = JobState::Running;
        self.cv.notify_all();
        true
    }

    /// Restores a job loaded from disk as paused; a resume is required before
    /// the engine is allowed to write again.
    pub fn restore_paused(&self) {
        let mut inner = self.lock();
        inner.state = JobState::Paused;
        inner.paused_blocks = 0;
        inner.active_blocks = 0;
        self.pause.store(true, Ordering::SeqCst);
        self.stop.store(false, Ordering::SeqCst);
        self.cv.notify_all();
    }

    /// Restores a terminal or stopped state loaded from disk.
    pub fn restore_state(&self, state: JobState) {
        let mut inner = self.lock();
        inner.state = state;
        inner.paused_blocks = 0;
        inner.active_blocks = 0;
        self.pause
            .store(state == JobState::Paused, Ordering::SeqCst);
        self.stop.store(false, Ordering::SeqCst);
        self.cv.notify_all();
    }

    pub fn set_active_blocks(&self, n: usize) {
        let mut inner = self.lock();
        inner.active_blocks = n;
        inner.paused_blocks = inner.paused_blocks.min(n);
    }

    pub fn request_pause(&self) -> bool {
        let mut inner = self.lock();
        if inner.state != JobState::Running {
            return false;
        }
        self.pause.store(true, Ordering::SeqCst);
        inner.state = JobState::Pausing;
        self.cv.notify_all();
        true
    }

    pub fn request_resume(&self) -> bool {
        let mut inner = self.lock();
        if !matches!(
            inner.state,
            JobState::Paused | JobState::Pausing | JobState::Stopped
        ) {
            return false;
        }
        self.pause.store(false, Ordering::SeqCst);
        self.stop.store(false, Ordering::SeqCst);
        inner.paused_blocks = 0;
        inner.state = JobState::Running;
        self.cv.notify_all();
        true
    }

    pub fn request_stop(&self) -> bool {
        let mut inner = self.lock();
        if !matches!(
            inner.state,
            JobState::Running | JobState::Pausing | JobState::Paused
        ) {
            return false;
        }
        self.stop.store(true, Ordering::SeqCst);
        self.pause.store(false, Ordering::SeqCst);
        inner.state = JobState::Stopping;
        self.cv.notify_all();
        true
    }

    pub fn is_stop(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    pub fn is_pause(&self) -> bool {
        self.pause.load(Ordering::SeqCst)
    }

    /// Blocks the caller until pause is cleared, stop is requested, or the job
    /// completes. Called by block coordinators after committing a unit.
    pub fn pause_point(&self) {
        if !self.is_pause() || self.is_stop() {
            return;
        }
        let mut inner = self.lock();
        if !self.is_pause() || self.is_stop() {
            return;
        }
        inner.paused_blocks += 1;
        if inner.paused_blocks >= inner.active_blocks {
            inner.state = JobState::Paused;
        }
        self.cv.notify_all();
        while self.is_pause() && !self.is_stop() {
            inner = self.cv.wait(inner).unwrap_or_else(|e| e.into_inner());
        }
        inner.paused_blocks = inner.paused_blocks.saturating_sub(1);
        if inner.state == JobState::Paused {
            inner.state = JobState::Running;
        }
        self.cv.notify_all();
    }

    pub fn block_finished(&self) {
        let mut inner = self.lock();
        inner.active_blocks = inner.active_blocks.saturating_sub(1);
        if inner.state == JobState::Pausing && inner.paused_blocks >= inner.active_blocks {
            inner.state = JobState::Paused;
        }
        self.cv.notify_all();
    }

    /// Called by the engine when the run loop ends normally.
    pub fn finish(&self) {
        let mut inner = self.lock();
        let final_state = if self.stop.load(Ordering::SeqCst) {
            JobState::Stopped
        } else {
            JobState::Completed
        };
        if !inner.state.is_terminal() {
            inner.state = final_state;
        }
        self.cv.notify_all();
    }

    pub fn fail(&self, err: impl Into<String>) {
        let mut inner = self.lock();
        inner.error = Some(err.into());
        inner.state = JobState::Failed;
        self.cv.notify_all();
    }

    /// Wait until the state changes away from `state` or a timeout elapses.
    pub fn wait_for_state(&self, wanted: JobState, timeout: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        let mut inner = self.lock();
        while inner.state != wanted {
            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, _) = self
                .cv
                .wait_timeout(inner, deadline - now)
                .unwrap_or_else(|e| e.into_inner());
            inner = guard;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transitions() {
        let c = Control::new();
        assert_eq!(c.state(), JobState::Created);
        assert!(c.start());
        assert_eq!(c.state(), JobState::Running);
        assert!(c.request_pause());
        assert_eq!(c.state(), JobState::Pausing);
        assert!(c.request_resume());
        assert_eq!(c.state(), JobState::Running);
        assert!(c.request_stop());
        assert_eq!(c.state(), JobState::Stopping);
        c.finish();
        assert_eq!(c.state(), JobState::Stopped);
    }

    #[test]
    fn pause_requires_all_blocks() {
        let c = std::sync::Arc::new(Control::new());
        c.start();
        c.set_active_blocks(2);
        c.request_pause();
        let mut handles = Vec::new();
        for _ in 0..2 {
            let c2 = c.clone();
            handles.push(std::thread::spawn(move || c2.pause_point()));
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(c.state(), JobState::Paused);
        c.request_resume();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(c.state(), JobState::Running);
    }
}
