#![no_main]

use libfuzzer_sys::fuzz_target;
use spanner_adbc::fuzzing::{OptValue, ensure_scheme, exercise_database_options};

// Database options set through the C ABI are untrusted; setting and reading them must not panic.
fuzz_target!(|input: (Vec<(String, OptValue)>, String)| {
    let (ops, endpoint) = input;
    let _ = ensure_scheme(&endpoint);
    exercise_database_options(ops);
});
