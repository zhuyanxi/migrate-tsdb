//! Guards the metadata a published crate needs.
//!
//! These are cheap string checks, but they catch the failure mode where a
//! `Cargo.toml` edit drops `description` and `cargo publish` only notices after
//! the version number has been burned.

use std::path::{Path, PathBuf};

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn manifest_carries_the_publish_metadata() {
    let raw = std::fs::read_to_string(crate_dir().join("Cargo.toml")).unwrap();
    for key in [
        "description",
        "repository",
        "homepage",
        "documentation",
        "readme",
        "license",
        "keywords",
        "categories",
        "rust-version",
    ] {
        assert!(
            raw.contains(&format!("{key} =")) || raw.contains(&format!("{key}.")),
            "Cargo.toml is missing `{key}`"
        );
    }
    assert!(
        raw.contains("[package.metadata.docs.rs]"),
        "docs.rs metadata is missing"
    );
    assert!(
        !raw.contains("TODO"),
        "Cargo.toml still has a TODO placeholder"
    );
}

#[test]
fn the_license_is_mit() {
    let text = std::fs::read_to_string(crate_dir().join("LICENSE")).unwrap();
    assert!(
        text.starts_with("MIT License"),
        "LICENSE must be the MIT text"
    );
    assert!(text.contains("Permission is hereby granted, free of charge"));
    // The ported decoders keep their upstream license next to ours.
    let apache = std::fs::read_to_string(crate_dir().join("LICENSE-APACHE")).unwrap();
    assert!(apache.contains("Apache License"));
    let notice = std::fs::read_to_string(crate_dir().join("NOTICE")).unwrap();
    assert!(notice.contains("Prometheus"));
}

#[test]
fn readme_and_licence_ship_with_the_crate() {
    for name in ["README.md", "LICENSE", "LICENSE-APACHE", "NOTICE"] {
        let path: PathBuf = crate_dir().join(name);
        assert!(path.is_file(), "{name} is missing from the crate root");
        let size = std::fs::metadata(&path).unwrap().len();
        assert!(size > 100, "{name} looks empty ({size} bytes)");
    }
}

#[test]
fn vendored_fixture_is_present() {
    // `cargo package` only ships the crate directory, so the tests must not
    // depend on ../../fixtures/gen.
    let data = crate_dir().join("tests/data");
    assert!(data.join("chunks.json").is_file());
    assert!(data.join("block/ground_truth.json").is_file());

    let ground_truth: serde_json::Value =
        serde_json::from_slice(&std::fs::read(data.join("block/ground_truth.json")).unwrap())
            .unwrap();
    let ulid = ground_truth["ulid"].as_str().expect("ulid in ground truth");
    let block = data.join("block").join(ulid);
    for entry in ["meta.json", "index", "chunks"] {
        assert!(
            block.join(entry).exists(),
            "vendored block is missing {entry}"
        );
    }
}

#[test]
fn readme_examples_are_tagged_for_docs() {
    let readme = std::fs::read_to_string(crate_dir().join("README.md")).unwrap();
    assert!(readme.contains("# tsdb-reader"));
    assert!(
        readme.contains("```rust,no_run"),
        "README examples must be rust code blocks"
    );
    // The crate docs pull the README in, so it has to stay self-contained.
    assert!(
        Path::new(&crate_dir().join("README.md")).exists(),
        "README is included from src/lib.rs"
    );
}
