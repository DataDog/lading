// Cases that SHOULD trigger the lint (3+ segments, not in use statements). If
// custom_lints does NOT trigger on this file the tool is NOT working as
// intended.

fn example_violations() {
    // 3 segments - should trigger
    let config = target::Config::Pid;
    let server = generator::Server::new();
    let metrics = json::MetricKind::Counter;

    // 4 segments - should trigger
    let cache = block::Cache::fixed_with_max_overhead();
    let sampler = cgroup::Sampler::new();

    // 5+ segments - should trigger
    let very_long = foo::bar::baz::qux::deeply::nested::call();
    let another = project::module::submodule::function::call::chain();

    // Mixed with valid code
    let file = fs::File::open("valid"); // 3 segments - should trigger
    let violation = target::Config::Docker; // 3 segments - should trigger
}

// More examples in different contexts
impl SomeStruct {
    fn method(&self) {
        some::long::path::here().field; // should trigger
    }
}

// In match statements
fn match_example(value: SomeEnum) {
    match value {
        some::variant::Path => {} // should trigger
        other => {}
    }
}

// Complex expressions that should trigger
fn complex_cases() {
    // Method chaining - the path segments should trigger
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

// Macros with long paths
fn macro_violations() {
    some_macro!(with::long::path::args);
    format!("string with {}", some::path::variable);
}
