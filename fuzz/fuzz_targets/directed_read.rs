#![no_main]

use libfuzzer_sys::fuzz_target;

// The `spanner.directed_read` parser must reject bad input without panicking.
fuzz_target!(|value: &str| {
    spanner_adbc::fuzzing::parse_directed_read(value);
});
