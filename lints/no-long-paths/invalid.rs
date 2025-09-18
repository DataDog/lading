// Cases that SHOULD trigger the lint (3+ segments, not in use statements)

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
    let foo = Balk::close("valid"); // 2 segments - OK
    let file = fs::File::open("valid"); // 3 segments - should trigger
    let violation = target::Config::Docker; // 3 segments - should trigger
}

// More examples in different contexts
impl SomeStruct {
    fn method(&self) {
        self.field.some::long::path::here(); // should trigger
    }
}

// In match statements
fn match_example(value: SomeEnum) {
    match value {
        some::variant::Path => {}, // should trigger
        other => {}
    }
}
