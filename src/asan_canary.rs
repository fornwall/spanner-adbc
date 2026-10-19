//! Test-only AddressSanitizer check, compiled only under `--cfg asan_canary`.
//!
//! A passing validation suite does not prove that Rust's sanitizer instrumentation is enabled.
//! `scripts/run-adbc-validation.sh` sets the cfg in its `rust-asan` run and expects ASan to report
//! a `heap-buffer-overflow` at the Rust store below. The buffer is allocated by a clang
//! `-fsanitize=address` C++ program, which checks that both sides share the compiler-rt runtime
//! and shadow memory. A cfg gate rather than a feature keeps it out of `--all-features` builds.

/// Write one byte past the end of a C++-allocated buffer. Called by
/// `adbc-validation/asan_canary.cc`.
///
/// # Safety
///
/// `ptr` must point to an allocation of at least `len` bytes. The write at offset `len` is
/// undefined behavior by design; only call this in the sanitizer subprocess, which expects ASan to
/// terminate it.
#[unsafe(no_mangle)]
#[allow(unsafe_code)]
// `#[no_mangle]` exports the symbol, which the lint cannot see through the private module.
#[allow(unreachable_pub)]
pub extern "C" fn spanner_adbc_asan_canary(ptr: *mut u8, len: usize) {
    // `write_volatile` keeps the unread store from being optimized away.
    // SAFETY: deliberately out of bounds; see the Safety section.
    unsafe {
        let out_of_bounds = ptr.add(len);
        out_of_bounds.write_volatile(0xAA);
    }
}
