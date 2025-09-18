// Edge cases and boundary conditions

// Use statements with various formats (should all be excluded)
use {
    std::collections::{HashMap, BTreeMap},
    tokio::sync::broadcast::Sender,
    crate::generator::http::Server
};

use self::module::submodule::function;

// Complex expressions
fn complex_cases() {
    // Method chaining - only the path segments should trigger
    let result = some::long::path::to::function()
        .map(|x| x.process())
        .unwrap_or_default();

    // Nested calls
    let value = outer::function(inner::long::path::call());

    // Array/struct initialization
    let config = Config {
        field: some::module::Type::default(),
        other: valid_two::segments(),
    };

    // Turbofish syntax
    let parsed = some::parser::module::<String>::parse();
}

// Comments and strings should not trigger
fn strings_and_comments() {
    // This comment mentions some::long::path::here but shouldn't trigger
    let string = "some::long::path::in::string";
    let raw_string = r#"another::long::path::here"#;
}

// Macros
fn macro_cases() {
    println!("valid");
    some_macro!(with::long::path::args);
    format!("string with {}", some::path::variable);
}