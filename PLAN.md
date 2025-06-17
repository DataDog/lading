# Config Check subcommand Plan

## Overview
Add a `config-check` subcommand that validates configuration files and exits 0 for valid configs, 1 for invalid ones.

## Implementation

### Error Handling
- File access errors: `error!("Could not read config file '{}': {}", path, err)`
- YAML/schema errors: `error!("Configuration validation failed: {}", err)`
- Success: `info!("Configuration file is valid")`

## Success Criteria
- `lading config-check --config-path valid.yaml` → exit 0
- `lading config-check --config-path invalid.yaml` → exit 1 with descriptive error
- Environment variable `LADING_CONFIG` fallback works
- No code duplication, reuses existing error types and patterns
