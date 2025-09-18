// Cases that should NOT trigger the lint

// Two segments are fine
fn example_two_segments() {
    let file = fs::File::open("path");
    let result = http::Response::new();
    let data = json::Value::from(42);
}

// Use statements should be excluded (even with 3+ segments)
use std::collections::HashMap;
use tokio::sync::broadcast::Sender;
use crate::generator::http::Server;
use lading_payload::dogstatsd::metric::Counter;

// std:: paths should be excluded
fn example_std_paths() {
    let path = std::env::current_dir().unwrap();
    let file = std::fs::File::create("test.txt").unwrap();
    let thread = std::thread::Builder::new().spawn(|| {});
}

// tokio:: paths should be excluded
fn example_tokio_paths() {
    let rt = tokio::runtime::Builder::new_multi_thread().build();
    let file = tokio::fs::File::create("test.txt").await;
    let sender = tokio::sync::broadcast::channel(10);
}