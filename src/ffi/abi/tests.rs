use adbc_core::error::Status;

use super::*;

/// The status codes come from `adbc_core`; pin them to the header's values.
#[test]
fn status_codes_match_the_c_header() {
    assert_eq!(size_of::<AdbcStatusCode>(), 1);
    let header_codes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14];
    assert_eq!(
        [
            ADBC_STATUS_OK,
            ADBC_STATUS_UNKNOWN,
            ADBC_STATUS_NOT_IMPLEMENTED,
            ADBC_STATUS_NOT_FOUND,
            ADBC_STATUS_ALREADY_EXISTS,
            ADBC_STATUS_INVALID_ARGUMENT,
            ADBC_STATUS_INVALID_STATE,
            ADBC_STATUS_INVALID_DATA,
            ADBC_STATUS_INTEGRITY,
            ADBC_STATUS_INTERNAL,
            ADBC_STATUS_IO,
            ADBC_STATUS_CANCELLED,
            ADBC_STATUS_TIMEOUT,
            ADBC_STATUS_UNAUTHENTICATED,
            ADBC_STATUS_UNAUTHORIZED,
        ],
        header_codes
    );
    assert_eq!(
        [
            Status::Ok,
            Status::Unknown,
            Status::NotImplemented,
            Status::NotFound,
            Status::AlreadyExists,
            Status::InvalidArguments,
            Status::InvalidState,
            Status::InvalidData,
            Status::Integrity,
            Status::Internal,
            Status::IO,
            Status::Cancelled,
            Status::Timeout,
            Status::Unauthenticated,
            Status::Unauthorized,
        ]
        .map(AdbcStatusCode::from),
        header_codes
    );
}

/// Sizes and offsets printed by compiling adbc.h for a 64-bit target. The interior offsets
/// catch swapped fields of the same size, which the sizes alone would miss.
#[test]
#[cfg(target_pointer_width = "64")]
fn driver_vtable_matches_the_c_header() {
    assert_eq!(ADBC_DRIVER_1_0_0_SIZE, 232);
    assert_eq!(size_of::<AdbcDriver>(), 464);

    assert_eq!(std::mem::offset_of!(AdbcDriver, ConnectionGetInfo), 64);
    assert_eq!(
        std::mem::offset_of!(AdbcDriver, StatementSetSubstraitPlan),
        224
    );
    assert_eq!(std::mem::offset_of!(AdbcDriver, StatementSetOptionInt), 456);
}

#[test]
#[cfg(target_pointer_width = "64")]
fn the_remaining_structs_match_the_c_header() {
    assert_eq!(ADBC_ERROR_1_0_0_SIZE, 32);
    assert_eq!(size_of::<AdbcError>(), 48);
    assert_eq!(size_of::<AdbcPartitions>(), 40);
    assert_eq!(size_of::<AdbcErrorDetail>(), 24);
}
