use std::fmt;

/// The server answered with a non-success status.
#[derive(Debug)]
pub struct ApiError {
    pub status: u16,
    pub body: String,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let body = self.body.trim_end();
        if body.is_empty() {
            write!(f, "server returned HTTP {}", self.status)
        } else {
            write!(f, "{body} (HTTP {})", self.status)
        }
    }
}

impl std::error::Error for ApiError {}

/// The request never got a response: connection refused, DNS failure, timeout, ...
#[derive(Debug)]
pub struct ConnectionError(pub String);

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConnectionError {}

/// A composition failed to parse. Holds the rendered report, which is printed as is.
#[derive(Debug)]
pub struct ParseError(pub String);

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0.trim_end())
    }
}

impl std::error::Error for ParseError {}

/// Maps an error to the exit codes from the spec:
/// 1 local/usage, 2 connection/timeout, 3 HTTP 400, 4 HTTP 500, 5 HTTP 503.
/// Other 4xx statuses map to 3 and other 5xx statuses to 4.
pub fn exit_code(err: &anyhow::Error) -> i32 {
    if let Some(api) = err.downcast_ref::<ApiError>() {
        return match api.status {
            503 => 5,
            500..=599 => 4,
            400..=499 => 3,
            _ => 1,
        };
    }
    if err.downcast_ref::<ConnectionError>().is_some() {
        return 2;
    }
    1
}
