#![no_main]

use libfuzzer_sys::fuzz_target;

// Connection URI parsing and expansion must not panic. The `options` target cannot reach it.
fuzz_target!(|uri: &str| {
    spanner_adbc::fuzzing::expand_connection_uri(uri);
});
