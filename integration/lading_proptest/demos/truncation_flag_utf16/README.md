# Truncation Flag Bug — UTF-16-LE Demo

Self-contained reproduction of an upstream-`IsTruncated`-flag drop in the Datadog Agent's `CombiningAggregator`. Runs two e2e tests against a real DD Agent container and presents the input, config, output, and analysis side-by-side.

## TL;DR — What the bug is

`combining_aggregator.go:103` — `bucket.flush()` decides whether to add the `...TRUNCATED...` marker bytes and `truncated:<reason>` tag using only a size check:

```go
content, isTruncated := b.applyTruncation(msg, content, b.contentLen >= b.maxContentSize, truncatedReason)
```

It never reads `msg.ParsingExtra.IsTruncated`. Compare to `bucket.emitSingle()` at line 130 which OR's the upstream flag into the same decision:

```go
content, _ = b.applyTruncation(msg, content, len(content) > b.maxContentSize || msg.ParsingExtra.IsTruncated, "single_line")
```

When a byte-shrinking parser (e.g. `encodedtext` for UTF-16, or `kubernetes` for CRI) reduces the post-parse content below `maxContentSize` after the framer set `IsTruncated=true`, `bucket.flush()` silently drops both the marker and the tag.

## What the demo does

Runs two e2e cases against `datadog/agent:7.78.2-full`:

| Case | `auto_multi_line_detection` | Routing | Expected output |
|---|---|---|---|
| Control | `false` | Path A `SingleLineHandler` | `...TRUNCATED...` marker AND `truncated:single_line` tag — both PRESENT |
| Main | `true` | Path C `CombiningAggregator` | Both MISSING — this is the bug |

Same input bytes, same `max_message_size_bytes: 1024`, same agent version. Only routing differs.

The input is a ~700-char ASCII line encoded as UTF-16-LE. Raw bytes ≈ 1402 bytes (over the framer's 1024 limit, framer cuts and sets `IsTruncated`). After the `encodedtext` parser decodes UTF-16-LE to UTF-8, the content is ≈ 512 bytes (well under `maxContentSize`).

## Prerequisites

- Rust toolchain (`cargo`)
- Docker (Docker Desktop, Colima, or any other compatible runtime)
- `datadog/agent:7.78.2-full` image (the script will pull it if not present)
- ~2 minutes of runtime
- ~150 MB of disk for the agent image and temp artifacts

## Running

From this directory:

```bash
./run.sh
```

Output goes to `./truncation_flag_demo_output/` by default. To customize:

```bash
./run.sh --output-dir /tmp/dd_bug_demo \
         --agent-image datadog/agent:7.78.2-full \
         --docker-host unix:///var/run/docker.sock
```

Flags:
- `--output-dir <path>` — where to write the artifacts (default: `./truncation_flag_demo_output`)
- `--agent-image <image>` — agent image to test (default: `datadog/agent:7.78.2-full`)
- `--docker-host <socket>` — override Docker socket (auto-detects Colima if present)
- `-h` / `--help` — show usage

The script prints the full input, config, output, and analysis to stdout. The same artifacts are also written to `--output-dir` for later inspection.

### Colima/Lima users on macOS

If your `--output-dir` is outside `$HOME`, the script will fall back to writing test artifacts under `$HOME/.lading_proptest_tmp_demo/` (which Docker can bind-mount), then copy them to the final `--output-dir` at the end. This is because Colima/Lima only mount `$HOME` into the VM by default — Docker bind mounts from anywhere else fail.

To skip the fallback and write directly to your chosen location, point `--output-dir` somewhere under `$HOME`.

The underlying `LADING_TMP_BASE` env var controls where the Rust test code writes its per-case artifacts. The script sets this for you, but if you're running `cargo test` directly without the script, you can set it manually.

## Output structure

```
truncation_flag_demo_output/
├── control/                              # auto_multi_line_detection: false
│   ├── config/
│   │   ├── datadog.yaml                  # main agent config
│   │   └── conf.d/proptest.d/conf.yaml   # log source config
│   ├── logs/proptest.log                 # UTF-16-LE input bytes
│   ├── output.json                       # raw intake payloads (with ddtags)
│   ├── output_messages.txt               # one entry per line, byte counts
│   └── summary.txt                       # action sequence summary
└── main/                                 # auto_multi_line_detection: true (bug)
    └── ... (same layout)
```

To inspect after the run:

```bash
# Raw input bytes (UTF-16-LE)
xxd truncation_flag_demo_output/main/logs/proptest.log | head

# Output ddtags (look for absence of 'truncated:*' in main, presence in control)
python3 -c "import json; [print(e.get('ddtags')) for e in json.load(open('truncation_flag_demo_output/main/output.json'))]"
python3 -c "import json; [print(e.get('ddtags')) for e in json.load(open('truncation_flag_demo_output/control/output.json'))]"
```

## Expected console output

The `cargo test` invocation will exit non-zero because the main test FAILS — that's the bug demo. The script handles this and continues to render the analysis.

The final section explains:
- Pipeline trace (what each component does)
- The exact code divergence
- Conditions required for the bug to manifest
- Other invocation paths where the same bug applies

## Notes for the logs team

The bug exists in the `bucket.flush()` code path regardless of which parser is upstream. It's only OBSERVABLE when the parser is one that:

1. Successfully shrinks bytes on truncated input
2. Doesn't validate the input (e.g. JSON validation in `dockerfile` parser would fail on cut JSON, masking the bug)

Parsers that meet these criteria:
- `encodedtext` (UTF-16-LE/BE, SHIFT-JIS) — demonstrated here
- `kubernetes` (CRI format used by kubelet) — same byte-splitting design, same bug. Production impact: every pod log on every k8s node.

The control case (Path A) shows that the upstream flag is correctly honored by `SingleLineHandler.process()` — so the framer and the upstream pipeline are working. The divergence is purely inside `CombiningAggregator`.
