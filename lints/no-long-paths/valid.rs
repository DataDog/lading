// Cases that should NOT trigger the lint. If custom_lints triggers in this file
// the tool is NOT working as intended.

// Use statements should be excluded (even with 3+ segments)
use self::module::submodule::function;
use crate::generator::http::Server;
use foo::{bar::baz, bard::baz::bing, bing::foo};
use lading_payload::dogstatsd::metric::Counter;
use std::collections::{BTreeMap, HashMap};
use tokio::sync::broadcast::Sender;

// Two segments are fine
fn example_two_segments() {
    let config = Config::new();
    let server = Server::start();
    let file = File::open("path");
    let result = Response::ok();
}

// Comments and strings should not trigger
fn strings_and_comments() {
    // This comment mentions some::long::path::here but shouldn't trigger
    let string = "some::long::path::in::string";
    let raw_string = r#"another::long::path::here"#;
}

// Simple macros
fn macro_cases() {
    println!("valid");
    format!("string with {}", some_variable);
}
