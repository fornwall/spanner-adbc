use super::*;
use base64::Engine as _;
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread::JoinHandle;
use std::time::Instant;

const DEADLINE: Duration = Duration::from_secs(10);

struct Request {
    method: String,
    path: String,
    headers: HeaderMap,
    body: Vec<u8>,
}

struct CredentialFile(std::path::PathBuf);

impl CredentialFile {
    fn new(json: &Value) -> Self {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "spanner-adbc-test-credentials-{}-{unique}.json",
            std::process::id()
        ));
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.write_all(json.to_string().as_bytes()).unwrap();
        Self(path)
    }
}

impl Drop for CredentialFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn read_request(mut stream: &TcpStream) -> Request {
    stream.set_read_timeout(Some(DEADLINE)).unwrap();
    let mut bytes = Vec::new();
    let end = loop {
        if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break end + 4;
        }
        let mut buf = [0; 1024];
        let count = stream.read(&mut buf).unwrap();
        assert_ne!(count, 0, "incomplete HTTP request");
        bytes.extend_from_slice(&buf[..count]);
        assert!(bytes.len() < 65536, "oversized HTTP headers");
    };
    let text = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let mut lines = text.lines();
    let mut first = lines.next().unwrap().split_whitespace();
    let method = first.next().unwrap().to_owned();
    let path = first.next().unwrap().to_owned();
    let mut headers = HeaderMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').unwrap();
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value.trim()).unwrap(),
        );
    }
    let length = headers
        .get(http::header::CONTENT_LENGTH)
        .map_or(0, |v| v.to_str().unwrap().parse::<usize>().unwrap());
    assert!(length < 65536, "oversized HTTP body");
    let received = bytes.len() - end;
    bytes.resize(end + length, 0);
    stream.read_exact(&mut bytes[end + received..]).unwrap();
    Request {
        method,
        path,
        headers,
        body: bytes[end..].to_vec(),
    }
}

fn token_server(responses: Vec<(&str, u16, Value)>) -> (String, JoinHandle<Vec<Request>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let responses: Vec<_> = responses
        .into_iter()
        .map(|(path, status, body)| (path.to_owned(), status, body.to_string()))
        .collect();
    let thread = std::thread::spawn(move || {
        let deadline = Instant::now() + DEADLINE;
        responses
            .into_iter()
            .map(|(path, status, body)| {
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(Instant::now() < deadline, "missing token request: {path}");
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(e) => panic!("accept token request: {e}"),
                    }
                };
                let request = read_request(&stream);
                assert_eq!(request.path, path);
                stream.set_write_timeout(Some(DEADLINE)).unwrap();
                write!(
                    stream,
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
                request
            })
            .collect()
    });
    (url, thread)
}

fn user_json(url: &str) -> Value {
    json!({
        "type": "authorized_user",
        "client_id": "test-client",
        "client_secret": "test-client-secret",
        "refresh_token": "test-refresh-token",
        "token_uri": format!("{url}/token"),
        "quota_project_id": "source-project"
    })
}

fn access_token_response(token: &str) -> Value {
    json!({"access_token": token, "expires_in": 3600, "token_type": "Bearer"})
}

fn service_account_json() -> Value {
    // This generated fixture is not registered with any service account.
    json!({
        "type": "service_account", "project_id": "test-project",
        "private_key_id": "test-key", "private_key": include_str!("service-account-test-key.pem"),
        "client_email": "test@example.iam.gserviceaccount.com"
    })
}

async fn headers(credentials: &Credentials) -> HeaderMap {
    let resource = tokio::time::timeout(DEADLINE, credentials.headers(Extensions::new()))
        .await
        .expect("token fetch deadline")
        .expect("fetch token");
    let CacheableResource::New { data, entity_tag } = resource else {
        panic!("first request needs fresh headers");
    };
    let mut extensions = Extensions::new();
    extensions.insert(entity_tag);
    assert!(matches!(
        tokio::time::timeout(DEADLINE, credentials.headers(extensions))
            .await
            .unwrap()
            .unwrap(),
        CacheableResource::NotModified
    ));
    assert!(data[AUTHORIZATION].is_sensitive());
    data
}

#[test]
fn authorized_user_exchanges_refresh_token_and_caches_headers() {
    for quota in [None, Some("billing-project")] {
        let (url, server) =
            token_server(vec![("/token", 200, access_token_response("user-token"))]);
        new_runtime().unwrap().block_on(async {
            let credentials =
                build_credentials_from_json(&user_json(&url).to_string(), quota).unwrap();
            let headers = headers(&credentials).await;
            assert_eq!(headers[AUTHORIZATION], "Bearer user-token");
            assert_eq!(
                headers[QUOTA_PROJECT_HEADER],
                quota.unwrap_or("source-project")
            );
        });
        let requests = server.join().unwrap();
        assert_eq!(requests[0].method, "POST");
        let body = serde_json::from_slice::<Value>(&requests[0].body).unwrap();
        assert_eq!(body["grant_type"], "refresh_token");
        assert_eq!(body["client_id"], "test-client");
        assert_eq!(body["client_secret"], "test-client-secret");
        assert_eq!(body["refresh_token"], "test-refresh-token");
        assert!(body["scopes"].is_null());
    }
}

#[test]
fn impersonated_json_exchanges_source_credentials_and_keeps_quota_separate() {
    const IAM_PATH: &str = "/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken";
    let expiry = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
    let (url, server) = token_server(vec![
        ("/token", 200, access_token_response("source-token")),
        (
            IAM_PATH,
            200,
            json!({"accessToken": "impersonated-token", "expireTime": expiry}),
        ),
    ]);
    let json = json!({
        "type": "impersonated_service_account",
        "service_account_impersonation_url": format!("{url}{IAM_PATH}"),
        "source_credentials": user_json(&url),
        "delegates": ["projects/-/serviceAccounts/delegate@example.com"],
        "scopes": ["https://www.googleapis.com/auth/spanner.data"],
        "quota_project_id": "json-project"
    });
    new_runtime().unwrap().block_on(async {
        let credentials =
            build_credentials_from_json(&json.to_string(), Some("billing-project")).unwrap();
        let headers = headers(&credentials).await;
        assert_eq!(headers[AUTHORIZATION], "Bearer impersonated-token");
        assert_eq!(headers[QUOTA_PROJECT_HEADER], "billing-project");
    });
    let requests = server.join().unwrap();
    assert_eq!(requests[1].method, "POST");
    assert_eq!(requests[1].headers[AUTHORIZATION], "Bearer source-token");
    assert_eq!(requests[1].headers[QUOTA_PROJECT_HEADER], "source-project");
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[1].body).unwrap(),
        json!({
            "delegates": ["projects/-/serviceAccounts/delegate@example.com"],
            "scope": ["https://www.googleapis.com/auth/spanner.data"], "lifetime": "3600s"
        })
    );
}

#[test]
fn external_account_exchanges_url_subject_token_with_json_quota_or_override() {
    for quota in [None, Some("billing-project")] {
        let (url, server) = token_server(vec![
            ("/subject", 200, json!({"id_token": "test-subject-token"})),
            (
                "/sts",
                200,
                json!({
                    "access_token": "federated-token", "expires_in": 3600,
                    "token_type": "Bearer", "issued_token_type": "urn:ietf:params:oauth:token-type:access_token"
                }),
            ),
        ]);
        let json = json!({
            "type": "external_account", "audience": "test-audience",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": format!("{url}/sts"), "quota_project_id": "json-project",
            "credential_source": {
                "url": format!("{url}/subject"), "headers": {"Metadata": "True"},
                "format": {"type": "json", "subject_token_field_name": "id_token"}
            }
        });
        new_runtime().unwrap().block_on(async {
            let credentials = build_credentials_from_json(&json.to_string(), quota).unwrap();
            let headers = headers(&credentials).await;
            assert_eq!(headers[AUTHORIZATION], "Bearer federated-token");
            assert_eq!(
                headers[QUOTA_PROJECT_HEADER],
                quota.unwrap_or("json-project")
            );
        });
        let requests = server.join().unwrap();
        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].headers["metadata"], "True");
        assert_eq!(requests[1].method, "POST");
        let body = String::from_utf8(requests[1].body.clone()).unwrap();
        for field in [
            "subject_token=test-subject-token",
            "audience=test-audience",
            "subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Ajwt",
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange",
        ] {
            assert!(body.split('&').any(|part| part == field), "{body}");
        }
    }
}

#[test]
fn service_account_signs_a_scoped_jwt_offline() {
    new_runtime().unwrap().block_on(async {
        let credentials = build_credentials_from_json(
            &service_account_json().to_string(),
            Some("billing-project"),
        )
        .unwrap();
        let headers = headers(&credentials).await;
        assert_eq!(headers[QUOTA_PROJECT_HEADER], "billing-project");
        let token = headers[AUTHORIZATION]
            .to_str()
            .unwrap()
            .strip_prefix("Bearer ")
            .unwrap();
        let parts: Vec<_> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        let decode = |part| {
            serde_json::from_slice::<Value>(
                &base64::prelude::BASE64_URL_SAFE_NO_PAD
                    .decode(part)
                    .unwrap(),
            )
            .unwrap()
        };
        assert_eq!(decode(parts[0])["alg"], "RS256");
        assert_eq!(decode(parts[0])["kid"], "test-key");
        let claims = decode(parts[1]);
        assert_eq!(claims["iss"], "test@example.iam.gserviceaccount.com");
        assert_eq!(
            claims["scope"],
            "https://www.googleapis.com/auth/cloud-platform"
        );
        assert!(claims["exp"].as_i64().unwrap() > chrono::Utc::now().timestamp());
        assert!(!parts[2].is_empty());
    });
}

#[test]
fn adc_loads_a_service_account_file_and_prefers_the_environment_quota_project() {
    const CHILD: &str = "SPANNER_ADBC_TEST_ADC_QUOTA_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let file = CredentialFile::new(&service_account_json());
        let name = "driver::credentials::tests::flows::adc_loads_a_service_account_file_and_prefers_the_environment_quota_project";
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"])
            .env(CHILD, "1")
            .env("GOOGLE_APPLICATION_CREDENTIALS", &file.0)
            .env("GOOGLE_CLOUD_QUOTA_PROJECT", "environment-project")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(
            stdout.contains("1 passed"),
            "the child ran no test: {stdout}"
        );
        return;
    }
    new_runtime().unwrap().block_on(async {
        let credentials = build_adc(Some("option-project")).unwrap();
        let headers = headers(&credentials).await;
        assert_eq!(headers[QUOTA_PROJECT_HEADER], "environment-project");
        assert!(
            headers[AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("Bearer ey")
        );
    });
}

#[test]
fn permanent_token_failure_is_unauthenticated_without_response_secrets() {
    let (url, server) = token_server(vec![(
        "/token",
        400,
        json!({
            "error": "invalid_grant", "error_description": "SECRET-REFRESH-TOKEN"
        }),
    )]);
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.endpoint = Some("https://127.0.0.1:1".into());
    db.connect_timeout_secs = Some(5.0);
    db.keyfile_json = Some(user_json(&url).to_string());
    let error = db.connect_with(None).unwrap_err();
    assert_eq!(error.status, Status::Unauthenticated, "{}", error.message);
    assert!(!error.message.contains("SECRET"), "{}", error.message);
    assert!(!error.message.contains("timed out"), "{}", error.message);
    assert_eq!(server.join().unwrap().len(), 1);
}
