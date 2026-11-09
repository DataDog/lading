# Property Reference

Detailed documentation for each property assertion used in `lading_proptest`. Each property checks a specific relationship between the test input (`LogBatch`) and agent output (`Vec<ReceivedLogEntry>`).

## Input Correlation Model

Every log line we write embeds a UUID that lets us correlate inputs with outputs after the agent processes them. The agent treats these markers as opaque content — it doesn't know about them.

### Markers

**`[PROPTEST:<uuid>]`** — Embedded in every header/standalone line. Appears at the start of the line (before any content) so it survives truncation. Used by all non-JSON formats.

**`"proptest_id":"<uuid>"`** — The JSON equivalent. A field in every JSON object we generate. Survives `json.Compact()` since it's a valid JSON field.

**`[CONT:<header-uuid>:<seq>]`** — Embedded in timestamp multiline continuation lines. The `header-uuid` ties it to its parent entry. The `seq` (0, 1, 2, ...) tracks ordering. After aggregation, these markers appear inside the parent entry's message joined with literal `\n`.

### Line IDs

Each `LogLine` in a `LogBatch` has an `id` field used internally for tracking (not written to the file — only the `content` is written). The ID conventions:

| Pattern | Meaning | Example |
|---|---|---|
| `<uuid>` | Header or standalone line | `a1b2c3d4-...` |
| `<uuid>:cont:<N>` | Timestamp multiline continuation | `a1b2c3d4-...:cont:0` |
| `<uuid>:json_line:<N>` | JSON multiline fragment | `a1b2c3d4-...:json_line:1` |

Properties filter out `:cont:` and `:json_line:` IDs when checking delivery — these fragments merge into their parent entry and aren't expected as standalone output entries.

### Metadata on LogBatch

- **`expected_continuations: Vec<(String, usize)>`** — For timestamp multiline: maps each header UUID to its expected continuation count. Used by `MultilineAggregated` to verify completeness.
- **`expected_json: Option<Vec<(String, serde_json::Value)>>`** — For JSON multiline: maps each UUID to the expected parsed JSON value. Used by `JsonIntegrity` to verify field preservation after compaction.

---

## AllLinesDelivered

**Intent:** No data loss — every log entry we wrote to the file shows up somewhere in the agent's output.

**How it works:**
1. Collects all input UUIDs, filtering out fragment IDs (`:cont:` and `:json_line:` suffixes — these are internal line fragments that get merged into their parent entry)
2. Scans every output message for ALL `[PROPTEST:uuid]` and `"proptest_id":"uuid"` patterns using `extract_all_ids` (not just the first match per message — important because aggregated messages can contain multiple UUIDs)
3. Set difference: any input UUID not found in any output message is "missing"

**Used by:** Truncation, Multiline, JSON Multiline, Mixed Multiline

**Known limitations:**
- The Apache path detection (`GET /<uuid>/`) in `extract_all_ids` only finds the first Apache UUID per message. If two Apache entries got merged (unlikely since Apache lines have timestamps that trigger `startGroup`), the second UUID could be missed.

---

## NoExtraLines

**Intent:** The agent doesn't fabricate data — no output entry has a UUID we didn't send.

**How it works:**
1. Same input ID filtering as `AllLinesDelivered` (skips `:cont:` and `:json_line:`)
2. For each output entry, extracts the **first** UUID only (uses `extract_id`, not `extract_all_ids`)
3. If that UUID isn't in the input set, it's "extra"

**Used by:** Currently not included in any scenario (available for future use).

**Known limitations:**
- Uses `extract_id` (first UUID only). If an aggregated output message has multiple UUIDs, only the first is checked against the input set. A hypothetical bug where the agent injects a fabricated UUID as a non-first UUID in an aggregated message would not be caught. Very unlikely to matter in practice.

---

## ContentPreserved

**Intent:** For non-truncated, non-multiline lines, the content passes through the agent unchanged.

**How it works:**
1. Builds a map of input UUID → input content (skips `:cont:` lines)
2. For each output entry, extracts the first UUID, looks up the expected input content
3. Checks bidirectional containment: either the output contains the input OR the input contains the output
4. Skips entries with `TRUNCATED` in the message (those are handled by `TruncationRespected`)

**Used by:** Truncation

**Known limitations:**
- **The bidirectional check is loose.** The reverse check (`expected.contains(output)`) could mask data loss where the agent delivered only a small fragment of the input. The intent is to allow the agent to wrap content with metadata, but a tiny output would incorrectly pass.
- **Doesn't filter `:json_line:` IDs in the input map.** Not a bug — JSON fragment IDs won't match any output UUID so they're harmlessly skipped — but inconsistent with other properties.
- **Doesn't verify byte-exact content.** Checks containment, not equality. If the agent modified characters within the content but preserved the overall string, this wouldn't catch it.

---

## TruncationRespected

**Intent:** Lines over the configured `max_message_size_bytes` get truncated. Lines under the limit arrive intact.

**How it works:**
1. Skips output entries starting with `...TRUNCATED...` (tail chunks from split messages — these are expected byproducts)
2. For each output entry with a UUID, looks up the original input line length
3. **If input was over the limit:**
   - Head chunk must be ≤ `max_message_bytes + 15` (the `...TRUNCATED...` marker is 15 bytes, appended by the agent)
   - Head chunk must end with the literal string `...TRUNCATED...`
4. **If input was under the limit:**
   - Output must be at least 50% of input length (catches severe data loss while allowing minor agent overhead)

**Used by:** Truncation

**Known limitations:**
- **The 50% threshold for under-limit lines is arbitrary.** An output at 51% of input length passes, which is still significant data loss. A tighter check (e.g., output length ≥ input length - 100 bytes) would catch more issues but risks false positives from agent metadata.
- **Doesn't verify content of under-limit lines.** Checks size only, not that the actual bytes match. `ContentPreserved` is included alongside this property in the truncation scenario to cover content verification.

---

## MultilineAggregated

**Intent:** Continuation lines are merged with the correct header, all expected continuations are present, and they appear in order.

**How it works:**
1. If `expected_continuations` is empty, passes trivially (non-multiline scenario)
2. Builds a map of header UUID → expected continuation count from `LogBatch.expected_continuations`
3. For each output entry with a UUID that's in the expected map:
   - Extracts all `[CONT:header_id:seq]` markers from the output message
   - **No cross-contamination:** every continuation's `header_id` must match the entry's UUID
   - **Completeness:** every expected sequence number (0 through count-1) must be present
   - **Ordering:** sequence numbers must appear in ascending order

**Used by:** Multiline, Mixed Multiline

**Known limitations:**
- **Doesn't verify continuation content.** Checks that markers are present and ordered, but doesn't verify the text between markers. If the agent corrupted continuation content but preserved the `[CONT:...]` markers, this wouldn't catch it.
- **Doesn't check for unexpected extra continuations.** If the output had continuations 0-5 but we only expected 0-2, the extras aren't flagged. In practice this can't happen since we control the input, but it's a gap in the assertion.

---

## ExpectedEntryCount

**Intent:** The number of logical output entries matches how many we generated.

**How it works:**
1. Counts output entries where `extract_id` returns `Some` (filters out agent internal messages, `...TRUNCATED...` tail chunks, and any other entries without a proptest UUID)
2. Compares to the expected count passed at construction

**Used by:** Multiline (timestamp-based)

**Known limitations:**
- **Counts by first UUID only.** If a plain text line merges into another entry, the count is correct (merged entry counts as 1). If a truncated line produces a head + tail, only the head has a UUID so it counts as 1. This is the intended behavior.
- **Not suitable for truncation or JSON scenarios.** Truncation produces extra tail chunks. JSON `TopLevelArray` entries get split into multiple outputs. These would throw off the count. That's why this property is only used by the multiline timestamp scenario where the entry count is predictable.

---

## JsonIntegrity

**Intent:** Every JSON entry arrives in the output as valid JSON with all expected fields and values preserved after the agent's compaction.

**How it works:**
1. If `expected_json` is `None`, passes trivially (non-JSON scenario)
2. For each expected `(uuid, json_value)` pair:
   - Finds the output entry containing that UUID by string search
   - Parses the output message as JSON
   - Verifies the `proptest_id` field matches
   - Recursively compares all fields:
     - **Objects:** checks every expected key is present with the correct value (key-by-key)
     - **Arrays:** checks length matches, then compares element-by-element
     - **Primitives:** checks equality
3. Reports specific failures with dotted field paths (e.g., `data.f_0`, `items[2]`)

**Used by:** JSON Multiline, Mixed Multiline

**Known limitations:**
- **Finds output by string containment.** `entry.message.contains(expected_id)` could theoretically match a UUID appearing as a substring of another value. UUIDs are 36 chars with a specific dash pattern so false matches are extremely unlikely.
- **One-directional field check (expected ⊆ actual).** If the agent added extra fields not in the expected JSON, this wouldn't catch it. This is intentional — the agent may add metadata fields like tags.
- **TopLevelArray error message is confusing.** When the expected JSON is `[{...}]` but the agent outputs `{...}` (because top-level arrays aren't supported), the root-level comparison reports `field '' expected [...], got {...}` — the empty path `''` is unclear. The failure is correctly detected but the diagnostic could be improved.
