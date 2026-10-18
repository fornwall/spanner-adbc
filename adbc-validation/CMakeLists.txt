# Build the Apache Arrow ADBC C++ validation suite against the shared driver.
# See README.md for prerequisites and usage.
cmake_minimum_required(VERSION 3.20)
project(spanner_adbc_validation LANGUAGES C CXX)

# Match the C++ standard required by the pinned arrow-adbc headers and tests.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Apply sanitizer flags before FetchContent so the driver manager and validation
# libraries inherit them. A separately built Rust cdylib needs its own instrumentation;
# ADBC_VALIDATION_RUST_SANITIZE in scripts/run-adbc-validation.sh enables it and
# selects clang so both sides share the compiler-rt ASan runtime.
set(SPANNER_VALIDATION_SANITIZE ""
    CACHE STRING "comma-separated -fsanitize list for the C++ suite (e.g. address,undefined)")
if(SPANNER_VALIDATION_SANITIZE)
  message(STATUS "Sanitizers: -fsanitize=${SPANNER_VALIDATION_SANITIZE}")
  add_compile_options(-fsanitize=${SPANNER_VALIDATION_SANITIZE}
                      -fno-omit-frame-pointer -fno-sanitize-recover=all)
  add_link_options(-fsanitize=${SPANNER_VALIDATION_SANITIZE})
endif()

# Keep this revision in sync with adbc_core, adbc_ffi and adbc_driver_manager
# in Cargo.toml. The suite uses the upstream SQL-rewriting and identifier-quoting hooks.
# FORCE replaces revisions cached in restored or incremental build trees.
set(ARROW_ADBC_TAG "0a40cf30d0458867cea48d0fbc5283340b59b40f"
    CACHE STRING "arrow-adbc revision (apache/arrow-adbc main, see above)" FORCE)

include(FetchContent)

# Build the driver manager and validation suite, using vendored dependencies.
set(ADBC_DRIVER_MANAGER ON CACHE BOOL "" FORCE)
set(ADBC_BUILD_TESTS ON CACHE BOOL "" FORCE)
set(ADBC_WITH_VENDORED_FMT ON CACHE BOOL "" FORCE)
set(ADBC_WITH_VENDORED_NANOARROW ON CACHE BOOL "" FORCE)
set(ADBC_BUILD_SHARED ON CACHE BOOL "" FORCE)
set(ADBC_BUILD_STATIC OFF CACHE BOOL "" FORCE)
set(ADBC_DRIVER_SQLITE OFF CACHE BOOL "" FORCE)
set(ADBC_DRIVER_POSTGRESQL OFF CACHE BOOL "" FORCE)
set(ADBC_DRIVER_FLIGHTSQL OFF CACHE BOOL "" FORCE)
set(ADBC_DRIVER_SNOWFLAKE OFF CACHE BOOL "" FORCE)
set(ADBC_DRIVER_BIGQUERY OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  arrow_adbc
  GIT_REPOSITORY https://github.com/apache/arrow-adbc.git
  GIT_TAG ${ARROW_ADBC_TAG}
  # A full clone is required to fetch a pinned commit behind the current branch tip.
  # The C/C++ project lives under c/ in the monorepo.
  SOURCE_SUBDIR c)
# Use the fetched GoogleTest: imported targets from a host installation can be
# scoped to arrow-adbc's subdirectory and unavailable to this harness.
set(CMAKE_DISABLE_FIND_PACKAGE_GTest TRUE)

FetchContent_MakeAvailable(arrow_adbc)

add_executable(spanner_validation spanner_validation.cc)
target_link_libraries(spanner_validation PRIVATE adbc_validation
                                                 adbc_driver_manager_shared
                                                 GTest::gtest_main)

# Apply strict warnings to this harness only; fetched third-party sources use
# their own warning policy. These flags require GCC or Clang.
target_compile_options(
  spanner_validation PRIVATE -Wall -Wextra -Wpedantic -Wshadow -Wconversion
                             -Wsign-conversion -Werror)
# Match the include layout the in-tree driver tests use.
target_include_directories(
  spanner_validation SYSTEM
  PRIVATE "${arrow_adbc_SOURCE_DIR}/c" "${arrow_adbc_SOURCE_DIR}/c/include"
          "${arrow_adbc_SOURCE_DIR}/c/vendor" "${arrow_adbc_SOURCE_DIR}/c/driver")

include(GoogleTest)
enable_testing()
gtest_discover_tests(spanner_validation)
