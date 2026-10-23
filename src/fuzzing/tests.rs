/// Keep fuzz harnesses, manifest entries and CI discovery in sync.
#[test]
fn every_fuzz_target_is_wired() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));

    // Each `[[bin]]` is immediately followed by `name = "..."`, the shape that
    // fuzz.yml's `discover` job parses.
    let manifest = std::fs::read_to_string(root.join("fuzz/Cargo.toml")).unwrap();
    let mut lines = manifest.lines();
    let mut declared = std::collections::BTreeSet::new();
    while let Some(line) = lines.next() {
        if line.trim() != "[[bin]]" {
            continue;
        }
        let name = lines
            .next()
            .expect("a [[bin]] section is followed by its name")
            .trim()
            .strip_prefix("name = \"")
            .and_then(|rest| rest.strip_suffix('"'))
            .expect("a [[bin]] section starts with `name = \"...\"`");
        declared.insert(name.to_string());
    }

    let harnesses: std::collections::BTreeSet<String> =
        std::fs::read_dir(root.join("fuzz/fuzz_targets"))
            .expect("fuzz_targets directory exists")
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
            .map(|path| path.file_stem().unwrap().to_str().unwrap().to_string())
            .collect();
    assert_eq!(
        declared, harnesses,
        "every fuzz_targets/<name>.rs needs a [[bin]] in fuzz/Cargo.toml and vice versa \
         (cargo-fuzz only runs declared targets, and fuzz.yml's matrix derives from them)"
    );

    let workflow = std::fs::read_to_string(root.join(".github/workflows/fuzz.yml")).unwrap();
    assert!(
        workflow.contains("fromJson(needs.discover.outputs.targets)"),
        "fuzz.yml must derive its matrix from fuzz/Cargo.toml via the `discover` job; a \
         hardcoded target list silently skips new targets"
    );
}

/// Run the partition oracle over the checked-in seed corpus, so a corpus/oracle mismatch
/// fails `cargo test --features fuzzing` rather than only a fuzz run.
#[test]
fn partition_seed_corpus_satisfies_the_oracle() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/seeds/partition");
    let mut verdicts = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(dir).expect("seed corpus directory exists") {
        let path = entry.unwrap().path();
        let bytes = std::fs::read(&path).unwrap();
        let name = path.file_name().unwrap().to_str().unwrap().to_string();
        verdicts.insert(name, super::decode_partition(&bytes));
    }
    let expected: std::collections::BTreeMap<String, bool> = [
        ("bare-bignum-query-descriptor", false),
        ("enveloped-bad-version", false),
        ("enveloped-bignum-query-descriptor", true),
        ("enveloped-query-descriptor", true),
        ("enveloped-query-with-row-type", true),
    ]
    .into_iter()
    .map(|(name, accepted)| (name.to_string(), accepted))
    .collect();
    assert_eq!(verdicts, expected);
}
