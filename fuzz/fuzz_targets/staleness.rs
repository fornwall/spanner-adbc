#![no_main]

use libfuzzer_sys::fuzz_target;

// Read-bound and duration parsing must reject bad input without panicking.
fuzz_target!(|value: &str| {
    spanner_adbc::fuzzing::parse_read_staleness(value);
    spanner_adbc::fuzzing::parse_duration(value);
});
