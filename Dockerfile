# Stage 0: Build — install dependencies into a venv
FROM docker.io/python:3.12-slim-bookworm AS builder
WORKDIR /app
RUN pip install --upgrade pip
COPY lading_py/ lading_py/
RUN pip install --prefix=/install lading_py/

# Stage 1: Runtime
FROM docker.io/python:3.12-slim-bookworm
COPY --from=builder /install /usr/local

# Smoke test
RUN lading-py --help

ENTRYPOINT ["lading-py"]
