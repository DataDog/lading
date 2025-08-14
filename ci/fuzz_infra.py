#!/usr/bin/env python3

"""
Helper for running fuzz targets in the internal fuzzing infrastructure.

If you want to run this locally, please set the VAULT_FUZZING_TOKEN environment variable (i.e: ddtool auth token security-fuzzing-platform)
"""

import os
from subprocess import Popen, PIPE
import requests
import toml
DEFAULT_FUZZING_SLACK_CHANNEL = "fuzzing-ops"
# Lets reuse the token for all requests to avoid issues.
# The process should be short lived enough that the token should be valid for the duration.
_cached_token = None

def get_auth_header():
    global _cached_token
    if os.getenv("VAULT_FUZZING_TOKEN") is not None:
        return os.getenv("VAULT_FUZZING_TOKEN")

    if _cached_token is None:
        _cached_token = os.popen('vault read -field=token identity/oidc/token/security-fuzzing-platform').read().strip()
    return _cached_token

def get_commit_sha():
    return os.getenv("CI_COMMIT_SHA")

def upload_fuzz(directory, git_sha, fuzz_test, team="single-machine-performance", core_count=2, duration=3600, proc_count=2, fuzz_memory=4):
    """
    This builds and uploads fuzz targets to the internal fuzzing infrastructure.
    It needs to be passed the -fuzz flag in order to build the fuzz with efficient coverage guidance.
    """

    api_url = "https://fuzzing-api.us1.ddbuild.io/api/v1"

    # Get the auth token a single time and reuse it for all requests
    auth_header = get_auth_header()
    if not auth_header:
        print('❌ Failed to get auth header')
        exit(1)

    max_pkg_name_length = 50

    pkgname_prefix = "lading-"
    pkgname = (pkgname_prefix + directory + "-" + fuzz_test).replace("_", "-").replace("/", "-")
    print(f'pkgname: {pkgname}')

    print(f'Getting presigned URL for {pkgname}...')
    headers = {"Authorization": f"Bearer {auth_header}"}
    presigned_response = requests.post(
        f"{api_url}/apps/{pkgname}/builds/{git_sha}/url", headers=headers, timeout=30
    )
    
    if not presigned_response.ok:
        print(f'❌ Failed to get presigned URL (status {presigned_response.status_code})')
        try:
            error_detail = presigned_response.json()
            print(f'Error details: {error_detail}')
        except:
            print(f'Raw error response: {presigned_response.text}')
        presigned_response.raise_for_status()
    presigned_url = presigned_response.json()["data"]["url"]

    print(f'Uploading {pkgname} ({fuzz_test}) for {git_sha}...')
    # Upload file to presigned URL
    with open(f'{directory}/target/x86_64-unknown-linux-gnu/release/{fuzz_test}', 'rb') as f:
        upload_response = requests.put(presigned_url, data=f, timeout=300)
        
        if not upload_response.ok:
            print(f'❌ Failed to upload file (status {upload_response.status_code})')
            try:
                error_detail = upload_response.json()
                print(f'Error details: {error_detail}')
            except:
                print(f'Raw error response: {upload_response.text}')
            upload_response.raise_for_status()

    print(f'Starting fuzzer for {pkgname} ({fuzz_test})...')
    # Start new fuzzer
    run_payload = {
        "app": pkgname,
        "debug": False,
        "version": git_sha,
        "core_count": core_count,
        "duration": duration,
        "type": "cargo-fuzz",
        "binary": fuzz_test,
        "team": team,
        "process_count": proc_count,
        "memory": fuzz_memory,
        "repository_url": "https://github.com/DataDog/lading",
        "slack_channel": DEFAULT_FUZZING_SLACK_CHANNEL,
    }

    headers = {"Authorization": f"Bearer {auth_header}", "Content-Type": "application/json"}
    response = requests.post(f"{api_url}/apps/{pkgname}/fuzzers", headers=headers, json=run_payload, timeout=30)
    
    if not response.ok:
        print(f'❌ API request failed with status {response.status_code}')
        try:
            error_detail = response.json()
            print(f'Error details: {error_detail}')
        except:
            print(f'Raw error response: {response.text}')
        response.raise_for_status()
    print(f'✅ Started fuzzer for {pkgname} ({fuzz_test})...')
    response_json = response.json()
    print(response_json)


def search_fuzz_tests(directory) -> list[str]:
    fuzz_list_cmd = ['cargo', '+nightly', 'fuzz', 'list']
    process = Popen(fuzz_list_cmd, cwd=directory, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    
    if process.returncode != 0:
        print(f'❌ Failed to list fuzz tests in {directory}')
        print(f'Command: {" ".join(fuzz_list_cmd)}')
        print(f'Exit code: {process.returncode}')
        if stderr:
            print(f'Error output: {stderr.decode("utf-8")}')
        if stdout:
            print(f'Standard output: {stdout.decode("utf-8")}')
        return []
    
    return stdout.decode('utf-8').splitlines()

def build_fuzz(directory, fuzz_test) -> bool:
    build_cmd = ['cargo', '+nightly', 'fuzz', 'build', fuzz_test]
    return Popen(build_cmd, cwd=directory).wait() == 0

# We want to search for all crates in the repository.
# We can't simply run `cargo fuzz list` in the root directory.
def is_fuzz_crate(cargo_toml_path) -> bool:
    """Check if a Cargo.toml file has cargo-fuzz = true in its metadata."""
    try:
        with open(cargo_toml_path, 'r') as f:
            cargo_config = toml.load(f)
            return cargo_config.get('package', {}).get('metadata', {}).get('cargo-fuzz', False)
    except Exception as e:
        print(f'Warning: Could not parse {cargo_toml_path}: {e}')
        return False

def find_cargo_roots(directory) -> list[str]:
    print(f'Finding cargo roots in {directory}')
    cargo_roots = []
    for root, dirs, files in os.walk(directory):
        if "Cargo.toml" in files:
            cargo_toml_path = os.path.join(root, "Cargo.toml")
            if is_fuzz_crate(cargo_toml_path):
                print(f'Found fuzz cargo root: {root}')
                cargo_roots.append(root)
            else:
                print(f'Skipping non-fuzz cargo root: {root}')
    return cargo_roots

if __name__ == "__main__":
    cargo_roots = find_cargo_roots(os.getcwd())
    print(cargo_roots)
    git_sha = get_commit_sha()

    for cargo_root in cargo_roots:
        fuzz_tests = search_fuzz_tests(cargo_root)
        print(f'Found {len(fuzz_tests)} fuzz tests in {cargo_root}')
        if len(fuzz_tests) == 0:
            print(f'No fuzz tests found in {cargo_root}, skipping...')
            continue
        
        for fuzz_test in fuzz_tests:
            print(f'Building fuzz for {cargo_root}/{fuzz_test} ({git_sha})')
            err = build_fuzz(cargo_root, fuzz_test)
            if not err:
                print(f'❌ Failed to build fuzz for {cargo_root}/{fuzz_test} ({git_sha}). Skipping uploading.')
                continue

            # Make cargo_root relative to the root of the repository, so the generated target name is lading-<foldername>-<fuzz-test>
            # In the future, the api will support a custom path flag
            repo_root = os.path.abspath(os.getcwd())
            rel_cargo_root = os.path.relpath(cargo_root, repo_root)
            print(f'Uploading fuzz for {rel_cargo_root}/{fuzz_test} ({git_sha})')
            upload_fuzz(rel_cargo_root, git_sha, fuzz_test)
