#![no_main]

use libfuzzer_sys::fuzz_target;

// `read_partition` takes caller-supplied bytes. The checks live in the wrapper, where the
// client's `Partition` type is in scope.
fuzz_target!(|descriptor: &[u8]| {
    let _ = spanner_adbc::fuzzing::decode_partition(descriptor);
});
