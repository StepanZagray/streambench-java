# Streambench Java

Independent Java HTTP streaming backend for [Streambench](https://github.com/StepanZagray/streambench-frontend).
This repository builds and runs by itself. Its checked-in text fixture matches the other three backends byte for byte. No model, Hugging Face login, or other repository is needed to deploy it.

## Run locally

From this repository root:

```sh
PORT=8082 ./run.sh
```

Environment: `PORT` (default 8080), `DATA_DIR` (default `data`), `MAX_STREAMS` (default 32).
Endpoints: `/health`, `/info`, `/stream?mode=paced&rate=50&count=500`.
See [the protocol](docs/protocol.md) and [fixture attribution/reproduction](data/README.md).
The frontend's local defaults use Go 8081, Java 8082, Rust 8083, Bun 8084.

## Deploy on Render

1. Choose **New → Web Service**, connect **this repository**, and use branch `main`.
2. Set **Language = Docker**, **Instance Type = Free**, and the same region for all backends (the included Blueprint uses Frankfurt).
3. Leave **Root Directory** blank. Use **Dockerfile Path = ./Dockerfile**, **Docker Build Context = .**, and leave **Docker Command** empty.
4. Set **Health Check Path = /health** and `MAX_STREAMS=32`. The server uses Render's `PORT` and binds on all interfaces.
5. Create the service. Copy its actual HTTPS URL into the frontend's Java endpoint field or `BACKEND_JAVA_URL` Pages build variable.

Alternatively create a Render Blueprint from this repository's `render.yaml`; it creates only this one Web Service. Do not create both a manual service and a Blueprint for the same backend.

Render builds and starts the container from the Dockerfile; separate build/start commands are unnecessary. See [Render Web Services](https://render.com/docs/web-services) and [Docker configuration](https://render.com/docs/docker).
Free instance hours are shared across the workspace; see [current free limits](https://render.com/docs/free).

```sh
docker build -t streambench-java .
docker run --rm -p 8080:8080 streambench-java
```

## Validation

```sh
mkdir -p build
JAVA_HOME=/usr/lib/jvm/java-21-openjdk
"$JAVA_HOME/bin/javac" --release 21 --add-modules jdk.httpserver -d build src/Streambench.java tests/StreambenchTest.java
"$JAVA_HOME/bin/java" --add-modules jdk.httpserver -cp build StreambenchTest
```

The frontend repository contains optional cross-backend protocol tests when all five repositories are checked out as siblings. Local native implementations have been tested; Docker image builds and Render deployment have not been verified because the local Docker socket was unavailable.

Additional implementation notes and standalone TCP checks: [docs/implementation.md](docs/implementation.md).
