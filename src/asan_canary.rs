//! Test-only AddressSanitizer check, compiled only under `--cfg asan_canary`.
//!
//! `scripts/run-adbc-validation.sh` sets this cfg in the `rust-asan` run. A cfg gate keeps the
//! exported function out of normal builds, including `--all-features` builds.
//!
//! A passing validation suite alone cannot prove that Rust's sanitizer instrumentation is
//! enabled. This positive control writes out of bounds from the Rust cdylib into a buffer
//! allocated by a clang `-fsanitize=address` C++ program. The validation script requires ASan
//! to report a `heap-buffer-overflow` at the Rust store.
//!
//! Using C++-allocated memory checks that both sides share the compiler-rt ASan runtime and
//! shadow memory: the instrumented Rust access must see the C++ allocator's poisoned redzone.

/// Test-only ASan check — **intentionally writes one byte out of bounds** and must not be
/// enabled in a shipped build.
///
/// Compiled only under `--cfg asan_canary` (the validation suite's `rust-asan` leg). Given a
/// `ptr`/`len` describing a C++-allocated buffer, it writes to `ptr[len]` — one byte past the end —
/// so that AddressSanitizer, sharing its runtime with the calling clang `-fsanitize=address` C++
/// program, reports a `heap-buffer-overflow` attributed to this Rust frame. Called by
/// `adbc-validation/asan_canary.cc`; see this module's docs for the rationale.
///
/// # Safety
///
/// `ptr` must point to an allocation of at least `len` bytes. The function then deliberately
/// performs an out-of-bounds write at offset `len`, which is undefined behaviour by construction.
/// Run it only in the sanitizer validation subprocess, which expects ASan to terminate it.
#[unsafe(no_mangle)]
#[allow(unsafe_code)]
// Deliberate out-of-bounds write; the crate-wide deny is overridden here only.
// Exported as a C symbol via `#[no_mangle]`, so `pub` is required even though the module is private
// and the lint cannot see the FFI export.
#[allow(unreachable_pub)]
pub extern "C" fn spanner_adbc_asan_canary(ptr: *mut u8, len: usize) {
    // Compute the one-past-the-end address and write to it. `write_volatile` keeps the compiler
    // from eliding the store (nothing reads it back), so ASan is guaranteed to see the access.
    // SAFETY: intentionally out of bounds — see the function's Safety section.
    unsafe {
        let out_of_bounds = ptr.add(len);
        out_of_bounds.write_volatile(0xAA);
    }
}
