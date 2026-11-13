//! CRC32C verification that compiles away without the `crc` feature.
//!
//! The call sites stay unconditional: they always pass the stored checksum and
//! the bytes it covers, and this module decides whether the comparison happens.

use crate::error::Result;

/// Compares `expected` with the CRC32C of `data` when `verify` is set.
///
/// Without the `crc` feature this is a no-op, so `verify` is ignored and the
/// `crc32c` crate is not linked at all.
#[cfg(feature = "crc")]
#[inline]
pub(crate) fn verify(
    verify: bool,
    expected: u32,
    data: &[u8],
    section: &'static str,
) -> Result<()> {
    if verify && crc32c::crc32c(data) != expected {
        return Err(crate::error::Error::Checksum(section));
    }
    Ok(())
}

/// No-op stand-in used when the crate is built without `crc`.
#[cfg(not(feature = "crc"))]
#[inline]
pub(crate) fn verify(
    _verify: bool,
    _expected: u32,
    _data: &[u8],
    _section: &'static str,
) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "crc")]
    #[test]
    fn detects_a_wrong_checksum() {
        let data = b"payload";
        let good = crc32c::crc32c(data);
        assert!(verify(true, good, data, "test").is_ok());
        assert!(matches!(
            verify(true, good ^ 0xFF, data, "test"),
            Err(crate::error::Error::Checksum("test"))
        ));
        // Disabled verification never looks at the checksum.
        assert!(verify(false, good ^ 0xFF, data, "test").is_ok());
    }

    #[cfg(not(feature = "crc"))]
    #[test]
    fn is_a_no_op_without_the_feature() {
        assert!(verify(true, 0xDEAD_BEEF, b"payload", "test").is_ok());
    }
}
