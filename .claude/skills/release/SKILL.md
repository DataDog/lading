---
name: release
description: Prepare a lading release. Updates CHANGELOG.md, bumps version in Cargo.toml, updates Cargo.lock, commits, tags, and pushes.
allowed-tools: Read, Edit, Bash, AskUserQuestion
---

# Release process for lading

## 1. Determine the next version

- Read `lading/Cargo.toml` and `CHANGELOG.md` to find the current version and unreleased changes.
- If the user hasn't specified a version, suggest one based on semver:
  - Breaking changes in the unreleased section -> bump minor (we're pre-1.0).
  - Otherwise bump patch.
- Confirm the version with the user before proceeding.

## 2. Update CHANGELOG.md

- Read `CHANGELOG.md`.
- Insert a new `## Unreleased` header (with empty subsections) above the current unreleased content.
- Rename the existing unreleased content block to `## [<version>]`.
- Ensure there is a blank line between the new empty `## Unreleased` and `## [<version>]`.

## 3. Bump version in Cargo.toml

- Edit `lading/Cargo.toml` to set `version = "<new_version>"`.

## 4. Update Cargo.lock

- Run `cargo check` to propagate the version change into the lockfile without re-resolving all dependencies.

## 5. Commit

- Stage `CHANGELOG.md`, `lading/Cargo.toml`, and `Cargo.lock`.
- Commit with message: `release: v<version>`

## 6. Tag and push

- Ask the user for confirmation before pushing.
- Create an annotated tag: `git tag -a v<version> -m "v<version>"`
- Push the commit and tag: `git push && git push --tags`
- The `release.yml` GitHub Actions workflow will automatically create a GitHub Release from the tag, pulling notes from CHANGELOG.md.
