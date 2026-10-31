//! Shared emulator, Omni and Cloud targets and setup for the Rust end-to-end suites.

use std::path::PathBuf;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use adbc_core::Database;
use google_cloud_lro::Poller;
use google_cloud_spanner::client::{Spanner, SpannerBuilderExt};
use google_cloud_spanner::omni::InstanceType;
use google_cloud_spanner_admin_instance_v1::model::Instance;

pub(crate) mod omni;

pub(crate) struct TestTarget {
    pub(crate) project: String,
    pub(crate) instance: String,
    pub(crate) database: String,
    pub(crate) is_emulator: bool,
    pub(crate) omni_endpoint: Option<String>,
}

impl TestTarget {
    pub(crate) fn database_path(&self) -> String {
        format!("{}/databases/{}", self.instance_path(), self.database)
    }

    pub(crate) fn instance_path(&self) -> String {
        format!("projects/{}/instances/{}", self.project, self.instance)
    }

    pub(crate) fn database_uri(&self) -> String {
        let mut uri = format!("spanner:///{}", self.database_path());
        if let Some(endpoint) = &self.omni_endpoint {
            let encoded: String = endpoint
                .bytes()
                .map(|byte| format!("%{byte:02X}"))
                .collect();
            uri.push_str(&format!(
                "?spanner.instance_type=omni&spanner.endpoint={encoded}"
            ));
        }
        uri
    }

    pub(crate) async fn spanner(&self) -> Spanner {
        let mut builder = Spanner::builder();
        if let Some(endpoint) = &self.omni_endpoint {
            builder = builder
                .with_instance_type(InstanceType::Omni)
                .with_endpoint(endpoint);
        }
        builder.build().await.expect("build setup Spanner client")
    }
}

/// Create the emulator instance and the test database if they are missing, then apply `ddl`,
/// which must be idempotent. Runs once per test binary: overlapping `create_instance` calls make
/// the emulator report `Instance not found`.
pub(crate) fn ensure_database_once(target: &TestTarget, ddl: &[&str]) {
    static DONE: Mutex<bool> = Mutex::new(false);
    // After a failed setup `done` stays false, so the next test retries it instead of every later
    // test failing on the poisoned lock.
    let mut done = DONE.lock().unwrap_or_else(PoisonError::into_inner);
    if !*done {
        tokio::runtime::Runtime::new()
            .expect("build setup runtime")
            .block_on(ensure_database(target, ddl));
        *done = true;
    }
}

async fn ensure_database(target: &TestTarget, ddl: &[&str]) {
    let ddl: Vec<String> = ddl.iter().map(|statement| statement.to_string()).collect();
    if let Some(endpoint) = &target.omni_endpoint {
        // scripts/with-omni.sh creates the database with Omni's CLI.
        if !ddl.is_empty() {
            omni::update_ddl(endpoint, target.database_path(), ddl, Vec::new())
                .await
                .expect("create the Omni test tables");
        }
        return;
    }
    let spanner = target.spanner().await;

    // Creation fails with `AlreadyExists` when an earlier run or test binary got there first.
    if target.is_emulator {
        let instance_admin = spanner
            .instance_admin_builder()
            .build()
            .await
            .expect("build instance admin client");
        let _ = instance_admin
            .create_instance()
            .set_parent(format!("projects/{}", target.project))
            .set_instance_id(&target.instance)
            .set_instance(
                Instance::new()
                    .set_config(format!(
                        "projects/{}/instanceConfigs/emulator-config",
                        target.project
                    ))
                    .set_display_name("ADBC test instance")
                    .set_node_count(1),
            )
            .poller()
            .until_done()
            .await;
    }
    let database_admin = spanner
        .database_admin_builder()
        .build()
        .await
        .expect("build database admin client");
    let _ = database_admin
        .create_database()
        .set_parent(target.instance_path())
        .set_create_statement(format!("CREATE DATABASE `{}`", target.database))
        .poller()
        .until_done()
        .await;
    if !ddl.is_empty() {
        database_admin
            .update_database_ddl()
            .set_database(target.database_path())
            .set_statements(ddl)
            .poller()
            .until_done()
            .await
            .expect("create the test tables");
    }
}

/// Open a connection, retrying briefly: a new emulator database can be invisible to the data
/// plane for a moment after `create_database` returns.
pub(crate) fn connect_with_retry<D: Database>(database: &D) -> D::ConnectionType {
    let mut last_err = None;
    for _ in 0..20 {
        match database.new_connection() {
            Ok(connection) => return connection,
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    }
    panic!("create connection failed after retries: {last_err:?}");
}

/// The cdylib that `cargo build` puts in `target/<profile>/`, one level above the test binary, or
/// `None` to skip. Panics instead under `ADBC_TEST_REQUIRE_TARGET`, so CI cannot pass the C ABI
/// tests without running them.
pub(crate) fn cdylib_path() -> Option<PathBuf> {
    let name = if cfg!(target_os = "windows") {
        "spanner_adbc.dll"
    } else if cfg!(target_os = "macos") {
        "libspanner_adbc.dylib"
    } else {
        "libspanner_adbc.so"
    };
    let path = std::env::current_exe().ok()?.parent()?.parent()?.join(name);
    if path.exists() {
        return Some(path);
    }
    assert!(
        !require_target(),
        "ADBC_TEST_REQUIRE_TARGET is set but {} is not built; run `cargo build` first",
        path.display()
    );
    None
}

pub(crate) fn require_target() -> bool {
    matches!(
        std::env::var("ADBC_TEST_REQUIRE_TARGET").ok().as_deref(),
        Some("1" | "true" | "yes")
    )
}

pub(crate) fn test_target() -> Option<TestTarget> {
    let env = |name| std::env::var(name).ok().filter(|value| !value.is_empty());
    let target = if let Some(endpoint) = env("SPANNER_OMNI_ENDPOINT") {
        Some(TestTarget {
            project: "default".into(),
            instance: "default".into(),
            database: "adbc-test".into(),
            is_emulator: false,
            omni_endpoint: Some(endpoint),
        })
    } else if env("SPANNER_EMULATOR_HOST").is_some() {
        Some(TestTarget {
            project: "test-project".into(),
            instance: "test-instance".into(),
            database: "adbc-test".into(),
            is_emulator: true,
            omni_endpoint: None,
        })
    } else {
        gcp_target()
    };
    assert!(
        target.is_some() || !require_target(),
        "ADBC_TEST_REQUIRE_TARGET is set: configure SPANNER_EMULATOR_HOST, \
         SPANNER_OMNI_ENDPOINT or SPANNER_GCP_DATABASE"
    );
    target
}

/// The Cloud Spanner database named by `SPANNER_GCP_DATABASE`, ignoring the local backends.
pub(crate) fn gcp_target() -> Option<TestTarget> {
    let spec = std::env::var("SPANNER_GCP_DATABASE")
        .ok()
        .filter(|value| !value.is_empty())?;
    let [project, instance, database] = spec.split('.').collect::<Vec<_>>()[..] else {
        panic!("SPANNER_GCP_DATABASE must use project.instance.database, got {spec:?}");
    };
    Some(TestTarget {
        project: project.into(),
        instance: instance.into(),
        database: database.into(),
        is_emulator: false,
        omni_endpoint: None,
    })
}
