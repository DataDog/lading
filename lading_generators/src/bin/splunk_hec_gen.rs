use argh::FromArgs;


fn default_config_path() -> String {
    "/etc/lading/splunk_hec_gen.toml".to_string()
}

#[derive(FromArgs, Debug)]
/// splunk_hec_gen options
struct Opts {
    /// config
    #[argh(option, default = "default_config_path()")]
    config_path: String,
}

fn main() {
    // read the config from a toml file
    println!("entered splunk hec generator!");
    println!("arguments from environment {:?}", argh::from_env::<Opts>());
    // spin up the data generation system

    // send data to Splunk, ack, loop
}

