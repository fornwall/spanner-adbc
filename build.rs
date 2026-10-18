//! Reads the `arrow-array` version from `Cargo.lock` for ADBC `DriverArrowVersion`.
//! Arrow does not export a version constant.

use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR");
    let lockfile = Path::new(&manifest_dir).join("Cargo.lock");
    println!("cargo:rerun-if-changed={}", lockfile.display());

    // Packaged sources exclude Cargo.lock. Always set the variable consumed by src/info.rs.
    let version = match fs::read_to_string(&lockfile) {
        // Reject an ambiguous or empty version when a lockfile is available.
        Ok(lock) => arrow_array_version(&lock).unwrap_or_else(|| {
            panic!(
                "could not resolve a unique `arrow-array` version from {}",
                lockfile.display()
            )
        }),
        // Without a readable lockfile, DriverArrowVersion reports "vunknown".
        Err(e) => {
            println!(
                "cargo:warning=could not read {} ({e}); reporting arrow-array version as \
                 \"unknown\" (expected for a source build from the published package, which \
                 excludes Cargo.lock)",
                lockfile.display()
            );
            "unknown".to_string()
        }
    };
    println!("cargo:rustc-env=SPANNER_ADBC_ARROW_VERSION={version}");
}

/// Returns the `arrow-array` version if exactly one non-empty entry is present.
fn arrow_array_version(lock: &str) -> Option<String> {
    let mut versions: Vec<&str> = Vec::new();
    let mut in_arrow_array = false;
    for line in lock.lines().map(str::trim) {
        if line == "[[package]]" {
            in_arrow_array = false;
        } else if line == "name = \"arrow-array\"" {
            in_arrow_array = true;
        } else if in_arrow_array
            && let Some(v) = line
                .strip_prefix("version = \"")
                .and_then(|rest| rest.strip_suffix('"'))
        {
            versions.push(v);
        }
    }
    match versions.as_slice() {
        [v] if !v.is_empty() => Some(v.to_string()),
        _ => None,
    }
}
