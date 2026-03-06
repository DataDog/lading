---
name: release
description: Prepare a lading release. Updates CHANGELOG.md, bumps version in Cargo.toml, updates Cargo.lock, commits, and creates a PR.
allowed-tools: Read, Edit, Bash, AskUserQuestion
---

# Release process for lading

## 1. Determine the next version

- Read `lading/Cargo.toml` and `CHANGELOG.md` to find the current version and unreleased changes.
- If the user hasn't specified a version, suggest one based on semver:
  - Breaking changes in the unreleased section -> bump minor (we're pre-1.0).
  - Otherwise bump patch.
- Confirm the version with the user before proceeding.

## 2. Create a release branch

- Create a branch from main: `git checkout -b prepare-release-v<version>`

## 3. Update CHANGELOG.md

- Read `CHANGELOG.md`.
- Insert a new `## Unreleased` header (with empty subsections) above the current unreleased content.
- Rename the existing unreleased content block to `## [<version>]`.
- Ensure there is a blank line between the new empty `## Unreleased` and `## [<version>]`.

## 4. Bump version in Cargo.toml

- Edit `lading/Cargo.toml` to set `version = "<new_version>"`.

## 5. Update Cargo.lock

- Run `cargo check` to propagate the version change into the lockfile without re-resolving all dependencies.

## 6. Commit and create PR

- Stage `CHANGELOG.md`, `lading/Cargo.toml`, and `Cargo.lock`.
- Commit with message: `release: v<version>`
- Push the branch and create a PR targeting main.

## 7. Tag after merge

- After the PR is merged, remind the user to tag the merge commit on main:
  - `git checkout main && git pull`
  - `git tag -a v<version> -m "v<version>"`
  - `git push origin v<version>`
- **NEVER use `git push --tags`** — always push exactly one tag at a time.
- The `release.yml` GitHub Actions workflow will automatically create a GitHub Release from the tag, pulling notes from CHANGELOG.md.
