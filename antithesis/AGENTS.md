This directory holds the files for running tests in Antithesis.

Use the `antithesis-setup` skill to scaffold and manage this directory. Use the `antithesis-research` skill to analyze the system and build a property catalog. Use the `antithesis-workload` skill to implement assertions and test commands. Use the `antithesis-launch` skill to build, validate, and submit Antithesis runs. Do not run `snouty launch` directly.

**snouty launch**
Use `snouty launch --json --webhook basic_test --config antithesis/config` to start an Antithesis run. Always run `compose build` first to keep images current.

**snouty validate**
Use this command to validate changes to the Antithesis scaffolding. See `snouty validate --help` for details.

**setup-complete.sh**
Inject this script into a Dockerfile to notify Antithesis that setup is complete. This script should only run once the system under test is ready for testing. Antithesis will not run any test commands until it receives this event.

**config**
This directory contains the `docker-compose.yaml` file that brings up this system in Antithesis, plus any related config files. Snouty pushes tagged images, consumes this config directory, and launches the run.

**test**
This directory contains test templates. A test template is a directory of test command executables. Each test command must have a valid prefix: `parallel_driver_, singleton_driver_, serial_driver_, first_, eventually_, finally_, anytime_`. Prefixes constrain when and how commands are composed in a single timeline. Files or subdirectories prefixed with `helper_` are ignored by Antithesis and may hold helper scripts alongside the commands.
