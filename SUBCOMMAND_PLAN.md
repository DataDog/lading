# Lading Subcommand Implementation Plan

## Overview

Add subcommand support to lading while maintaining 100% backwards compatibility using clap's two-parser fallback pattern.

## Goals

- Support both `lading [OPTIONS]` (current) and `lading run [OPTIONS]` (new)
- Zero breaking changes - existing scripts/workflows continue working
- Enable future extensibility with additional subcommands

## Implementation Strategy: Two-Parser Fallback

### Core Approach

Use separate parsers for subcommand and legacy modes, with automatic fallback:

```rust
// Parser for new subcommand structure
#[derive(Parser)]
struct CliWithSubcommands {
    #[command(subcommand)]
    command: Commands,
}

// Parser for legacy flat structure
#[derive(Parser)]
struct CliFlatLegacy {
    #[command(flatten)]
    args: LadingArgs,
}

// Shared arguments used by both modes
#[derive(Args)]
#[clap(group(ArgGroup::new("target").required(true).args(&["target_path", "target_pid", "target_container", "no_target"])))]
#[clap(group(ArgGroup::new("telemetry").required(true).args(&["capture_path", "prometheus_addr", "prometheus_path"])))]
struct LadingArgs {
    #[clap(long, default_value_t = default_config_path())]
    config_path: String,

    #[clap(long)]
    global_labels: Option<CliKeyValues>,

    // ... all other current Opts fields
    target_arguments: Vec<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run lading with specified configuration
    Run(RunCommand),
    // Future: Add more subcommands here
}

#[derive(Args)]
struct RunCommand {
    #[command(flatten)]
    args: LadingArgs,
}
```

### Main Function Updates

```rust
fn main() -> Result<(), Error> {
    // Existing setup unchanged
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(false)
        .finish()
        .init();

    let version = env!(\"CARGO_PKG_VERSION\");
    info!(\"Starting lading {version} run.\");

    // Two-parser fallback logic
    let args = match CliWithSubcommands::try_parse() {
        Ok(cli) => match cli.command {
            Commands::Run(run_cmd) => run_cmd.args,
            // Future: Handle other subcommands
        },
        Err(_) => {
            // Fall back to legacy parsing
            match CliFlatLegacy::try_parse() {
                Ok(legacy) => legacy.args,
                Err(err) => err.exit(),
            }
        }
    };

    // Everything below unchanged - single code path
    let config = get_config(&args, None)?;
    let experiment_duration = if args.experiment_duration_infinite {
        Duration::MAX
    } else {
        Duration::from_secs(args.experiment_duration_seconds.into())
    };

    // ... rest of existing main() logic
}
```

### Configuration Function Updates

```rust
// Update signature to accept LadingArgs instead of Opts
fn get_config(args: &LadingArgs, config: Option<String>) -> Result<Config, Error> {
    // Implementation unchanged, just s/ops/args/
}
```

## Implementation Steps

1. **Refactor Types**
   - Rename `Opts` → `LadingArgs`
   - Create `CliWithSubcommands` and `CliFlatLegacy` structs
   - Create `Commands` enum with `Run` variant

2. **Update Main**
   - Implement two-parser fallback logic
   - Update references from `ops` to `args`
   - Pass `&LadingArgs` to `get_config`

3. **Testing**
   - Verify `lading --config-path=...` works unchanged
   - Verify `lading run --config-path=...` works identically
   - Ensure all existing tests pass
   - Test help output for both modes

## Benefits

- **True backwards compatibility**: Legacy commands parse exactly as before
- **Clean separation**: Each parser handles its own mode without conflicts
- **Future-proof**: Easy to add new subcommands to `Commands` enum
- **Explicit behavior**: Clear fallback logic, no ambiguity
- **Maintains DRY**: Shared `LadingArgs` structure

## Future Subcommands

Once infrastructure is in place, we can add:
- `lading validate` - Validate configuration without running
- `lading generate` - Interactive config generation
- `lading inspect` - Analyze running targets

## Migration Timeline

- **Phase 1**: Implement two-parser support (this work)
- **Phase 2**: Document and promote `lading run` usage
- **Phase 3**: Add new subcommands as needed
- **Phase 4**: Consider deprecation of legacy format (1+ year)
