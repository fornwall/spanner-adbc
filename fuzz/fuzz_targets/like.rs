#![no_main]

use libfuzzer_sys::fuzz_target;
use regex::Regex;

// The get_objects LIKE matcher must finish quickly on `%`-heavy patterns and agree with a regex
// translation. ADBC LIKE has no escape character: only `%` and `_` are special.
fuzz_target!(|input: (String, String)| {
    let (pattern, value) = input;
    let got = spanner_adbc::fuzzing::like_match(&pattern, &value);

    // `(?s)` lets `.` match newlines; LIKE matches the whole value.
    let mut re = String::from(r"(?s)\A");
    for ch in pattern.chars() {
        match ch {
            '%' => re.push_str(".*"),
            '_' => re.push('.'),
            other => re.push_str(&regex::escape(&other.to_string())),
        }
    }
    re.push_str(r"\z");

    // A large pattern can exceed the regex size limit; skip the comparison then.
    let expected = match Regex::new(&re) {
        Ok(regex) => regex.is_match(&value),
        Err(_) => return,
    };
    assert_eq!(
        got, expected,
        "like_match disagreed with regex oracle for pattern {pattern:?} value {value:?}"
    );
});
