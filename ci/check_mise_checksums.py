#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["tomlkit"]
# ///
"""Check that mise.lock has checksums for all tools on all required platforms.

Usage:
    uv run ci/check_mise_checksums.py          # Report missing checksums
    uv run ci/check_mise_checksums.py --fix    # Download and add missing checksums
"""

import hashlib
import sys
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import tomlkit

REQUIRED_PLATFORMS = ("macos-arm64", "linux-arm64", "linux-x64")
LOCK_FILE = Path(__file__).resolve().parent.parent / "mise.lock"


@dataclass
class MissingChecksum:
    tool: str
    platform: str
    url: str


@dataclass
class FixFailure:
    entry: MissingChecksum
    reason: str


def fetch_checksum(url: str) -> str | None:
    """Download a URL and return its sha256 checksum."""
    try:
        with tempfile.NamedTemporaryFile() as tmp:
            with urllib.request.urlopen(url) as resp:
                while chunk := resp.read(8192):
                    tmp.write(chunk)
            tmp.seek(0)
            return hashlib.sha256(tmp.read()).hexdigest()
    except Exception as e:
        print(f"    FAILED to fetch {url}: {e}", file=sys.stderr)
        return None


def fix_lockfile(missing: list[MissingChecksum]) -> list[FixFailure]:
    """Download artifacts and insert checksums into mise.lock."""
    doc = tomlkit.parse(LOCK_FILE.read_text())
    failures: list[FixFailure] = []

    for entry in missing:
        print(f"  Fetching {entry.tool} ({entry.platform})... ", end="", flush=True)
        checksum = fetch_checksum(entry.url)
        if checksum is None:
            failures.append(FixFailure(entry, "failed to download artifact"))
            continue
        print("done")

        # Walk the parsed TOML to find the matching platform table and insert the checksum.
        tool_key = entry.tool.rsplit("@", 1)[0]
        platform_key = f"platforms.{entry.platform}"
        patched = False
        for tool_entry in doc["tools"][tool_key]:
            if platform_key in tool_entry and tool_entry[platform_key].get("url") == entry.url:
                tool_entry[platform_key]["checksum"] = f"sha256:{checksum}"
                patched = True
                break
        if not patched:
            failures.append(FixFailure(entry, "could not locate entry in lockfile"))

    LOCK_FILE.write_text(tomlkit.dumps(doc))
    print(f"\nUpdated {LOCK_FILE.name}.")
    return failures


def main() -> int:
    fix_mode = "--fix" in sys.argv

    doc = tomlkit.parse(LOCK_FILE.read_text())

    missing_platform: list[str] = []
    missing_checksum: list[MissingChecksum] = []

    for tool_name, entries in doc.get("tools", {}).items():
        if not isinstance(entries, list):
            entries = [entries]

        for entry in entries:
            version = entry.get("version", "unknown")
            label = f"{tool_name}@{version}"

            for platform in REQUIRED_PLATFORMS:
                key = f"platforms.{platform}"
                if key not in entry:
                    missing_platform.append(f"{label}: missing platform '{platform}'")
                elif "checksum" not in entry[key]:
                    url = entry[key].get("url", "")
                    missing_checksum.append(MissingChecksum(label, platform, url))

    errors = missing_platform + [
        f"{m.tool}: missing checksum for '{m.platform}'" for m in missing_checksum
    ]

    if errors:
        print(f"Found {len(errors)} issue(s) in {LOCK_FILE.name}:\n")
        for err in errors:
            print(f"  - {err}")
        print()

    if missing_platform:
        print(
            "Note: missing platforms cannot be auto-fixed. "
            "Run `mise lock` to regenerate the lockfile."
        )

    if fix_mode and missing_checksum:
        print(f"Fixing {len(missing_checksum)} missing checksum(s)...\n")
        failures = fix_lockfile(missing_checksum)
        if failures:
            print(f"\n{len(failures)} fix(es) failed:")
            for f in failures:
                print(f"  - {f.entry.tool} ({f.entry.platform}): {f.reason}")
            return 1
        return 1 if missing_platform else 0

    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
