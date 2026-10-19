//! Shared emulator, Omni and Cloud targets for the Rust end-to-end suites.

use google_cloud_spanner::client::Spanner;
use google_cloud_spanner::omni::{InstanceType, SpannerBuilderExt};

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
