use super::*;

#[test]
fn catches_a_panic_as_an_internal_error() {
    let error = catch(|| -> Result<()> { panic!("kaboom") }).unwrap_err();
    assert_eq!(error.status, Status::Internal);
    assert!(error.message.contains("kaboom"), "{}", error.message);
}

/// Both panic payload shapes `panic!` produces are rendered; anything else says so rather
/// than reporting an empty message.
#[test]
fn renders_both_panic_payload_shapes() {
    assert_eq!(panic_message(&"borrowed"), "borrowed");
    assert_eq!(panic_message(&"owned".to_string()), "owned");
    assert_eq!(panic_message(&7_u8), "unknown panic payload");
}

/// An interior NUL becomes a visible `?` rather than truncating what follows it.
#[test]
fn sanitized_cstring_replaces_interior_nuls_instead_of_truncating() {
    assert_eq!(
        sanitized_cstring("before\0after").to_bytes(),
        b"before?after"
    );
    assert_eq!(sanitized_cstring("clean").to_bytes(), b"clean");
    assert_eq!(sanitized_cstring("").to_bytes(), b"");
}

#[test]
fn passes_through_ordinary_results() {
    assert_eq!(catch(|| Ok(7)).unwrap().unwrap(), 7);
    let inner =
        catch(|| -> Result<()> { Err(Error::with_message_and_status("nope", Status::NotFound)) })
            .unwrap()
            .unwrap_err();
    assert_eq!(inner.status, Status::NotFound);
}

#[test]
fn write_string_and_bytes_report_the_required_size_without_writing() {
    let mut buffer = [0_i8; 2];
    let mut length = buffer.len();
    unsafe { write_string("hello", buffer.as_mut_ptr(), &raw mut length) }.unwrap();
    // "hello" plus NUL; nothing written because the buffer is too small.
    assert_eq!(length, 6);
    assert_eq!(buffer, [0, 0]);

    let mut buffer = [0_i8; 6];
    let mut length = buffer.len();
    unsafe { write_string("hello", buffer.as_mut_ptr(), &raw mut length) }.unwrap();
    assert_eq!(length, 6);
    assert_eq!(
        unsafe { CStr::from_ptr(buffer.as_ptr()) }.to_str().unwrap(),
        "hello"
    );

    // `write_bytes` follows the same protocol, without the trailing NUL.
    let mut buffer = [0_u8; 1];
    let mut length = buffer.len();
    unsafe { write_bytes(b"abc", buffer.as_mut_ptr(), &raw mut length) }.unwrap();
    assert_eq!(length, 3);
    assert_eq!(buffer, [0]);
}

#[test]
fn write_string_preserves_utf8_and_terminates_empty_values() {
    let sentinel: c_char = 120;
    for value in ["", "smörgås"] {
        let mut buffer = [sentinel; 16];
        let mut length = value.len() + 1;
        unsafe { write_string(value, buffer.as_mut_ptr(), &raw mut length) }.unwrap();
        assert_eq!(length, value.len() + 1);
        assert_eq!(
            unsafe { CStr::from_ptr(buffer.as_ptr()) }.to_bytes(),
            value.as_bytes()
        );
        assert_eq!(buffer.get(length), Some(&sentinel));
    }

    let mut buffer = [sentinel; 16];
    let mut length = buffer.len();
    let error =
        unsafe { write_string("left\0right", buffer.as_mut_ptr(), &raw mut length) }.unwrap_err();
    assert_eq!(error.status, Status::Internal);
    assert!(error.message.contains("byte 4"), "{}", error.message);
    assert_eq!(length, buffer.len());
    assert_eq!(buffer, [sentinel; 16]);
}

/// adbc.h promises that a reported length no larger than the one passed in means the value
/// was written, so a null buffer claiming room has to be refused rather than answered `OK`.
/// A null buffer of length zero is the size probe, and stays legal.
#[test]
fn a_null_buffer_is_a_size_probe_only_when_it_says_it_holds_nothing() {
    let mut length = 64;
    let error =
        unsafe { write_string("hello", std::ptr::null_mut(), &raw mut length) }.unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("64"), "{}", error.message);
    assert_eq!(length, 64, "a rejected call reports no size");

    let mut length = 0;
    unsafe { write_string("hello", std::ptr::null_mut(), &raw mut length) }.unwrap();
    assert_eq!(length, 6);

    let mut length = 0;
    unsafe { write_bytes(b"abc", std::ptr::null_mut(), &raw mut length) }.unwrap();
    assert_eq!(length, 3);
    assert!(
        unsafe { write_bytes(b"", std::ptr::null_mut(), &raw mut length) }.is_err(),
        "the length is now 3, so the null buffer claims room it does not have"
    );
}

#[test]
fn optional_str_maps_null_to_none_and_required_str_rejects_it() {
    assert_eq!(
        unsafe { optional_str(std::ptr::null(), "catalog") }.unwrap(),
        None
    );
    let value = CString::new("main").unwrap();
    assert_eq!(
        unsafe { optional_str(value.as_ptr(), "catalog") }.unwrap(),
        Some("main")
    );

    let error = unsafe { required_str(std::ptr::null(), "table_name") }.unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("table_name"), "{}", error.message);
}

#[test]
fn optional_str_list_reads_until_the_null_terminator() {
    let first = CString::new("TABLE").unwrap();
    let second = CString::new("VIEW").unwrap();
    let array = [first.as_ptr(), second.as_ptr(), std::ptr::null()];
    assert_eq!(
        unsafe { optional_str_list(array.as_ptr(), "table_type") }.unwrap(),
        Some(vec!["TABLE", "VIEW"])
    );
    assert_eq!(
        unsafe { optional_str_list(std::ptr::null(), "table_type") }.unwrap(),
        None
    );
}

#[test]
fn byte_slice_accepts_null_only_when_empty() {
    assert_eq!(
        unsafe { byte_slice(std::ptr::null(), 0, "plan") }.unwrap(),
        b""
    );
    assert!(unsafe { byte_slice(std::ptr::null(), 1, "plan") }.is_err());
    let data = [1_u8, 2, 3];
    assert_eq!(
        unsafe { byte_slice(data.as_ptr(), data.len(), "plan") }.unwrap(),
        &data
    );
}

#[test]
fn contains_recursively_panicking_payload_cleanup() {
    let error =
        catch(|| -> Result<()> { std::panic::panic_any(crate::ffi::test_support::PanicOnDrop) })
            .unwrap_err();
    assert_eq!(error.status, Status::Internal);
    assert!(error.message.contains("unknown panic payload"));
}

#[test]
fn releases_custom_panic_payloads_when_cleanup_succeeds() {
    let value = std::sync::Arc::new(());
    let payload = value.clone();
    let error = catch(|| -> Result<()> { std::panic::panic_any(payload) }).unwrap_err();
    assert_eq!(error.status, Status::Internal);
    assert_eq!(std::sync::Arc::strong_count(&value), 1);
}
