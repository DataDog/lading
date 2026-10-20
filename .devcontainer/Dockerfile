FROM mcr.microsoft.com/devcontainers/base:1-bookworm

COPY ci/update-ca-certificates /usr/local/bin/lading-update-ca-certificates
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get upgrade -y \
    && apt-get -y install ca-certificates git build-essential protobuf-compiler pkg-config libssl-dev fuse3 libfuse3-dev \
    && lading-update-ca-certificates \
    # Remove imagemagick due to https://security-tracker.debian.org/tracker/CVE-2019-10131
    && apt-get purge -y imagemagick imagemagick-6-common \
    && rm -rf /var/lib/apt/lists/*
