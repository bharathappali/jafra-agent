# Jafra Agent

`jafra-agent` version `0.1.0` is a node-local Rust collector. It discovers
JFR files under `/jfr-data/<namespace>/<podUID>/<container>/`, treats the
68-byte JFR header as the source of truth for chunk finalization, and either
logs finalized chunks or streams them to `jafra-analyzer`. The init container
writes `.jafra-identity.json` in that directory so the agent can send the
Kubernetes pod name with each chunk.

In `grpc` mode, after every chunk in a *rotated* file is `ACCEPTED` or
`DUPLICATE`, the agent deletes that source file. The live file the JVM is
still writing is never removed. Log-only mode never deletes recordings.

## Build

The image needs `protobuf-compiler` for `tonic-build`. `cargo test` still
works without a container as long as `protoc` is on `PATH`.

```bash
cargo test
cargo build --release
docker build -f jafra-agent/Dockerfile -t quay.io/bharathappali/jafra-agent:0.1.0 .
```

Build the container from the repository root so `contracts/jafra.proto` is
visible to `build.rs`.

## Modes

- `JAFRA_MODE=log-only` logs one structured `jfr_chunk_finalized` event per
  newly discovered finalized chunk. Source files are left in place.
- `JAFRA_MODE=grpc` streams `OpenChunk`, 128 KiB `ChunkFrame` messages, and
  `CommitChunk` to the analyzer. `ACCEPTED` and `DUPLICATE` count as durable
  acknowledgement. `RETRY` uses bounded exponential backoff. `REJECTED` is
  permanent and keeps the source file.

Frame order is preserved inside one stream. Chunks from the same physical
file are uploaded one at a time; different files may proceed in parallel.

Deletion requires all of:

1. Mode is `grpc` and `JAFRA_DELETE_CLOSED_FILES=true` (the default).
2. The file has no incomplete tail (`file size` equals the last finalized
   chunk end).
3. Every discovered chunk in that file was `ACCEPTED` or `DUPLICATE`.
4. A newer `profile-N.jfr` exists in the same directory, so this file is
   not the live recording.

## Deploy

```bash
kind load docker-image quay.io/bharathappali/jafra-agent:0.1.0 --name jafra
kubectl apply -f deploy/agent/rbac.yaml
kubectl apply -f deploy/agent/daemonset.yaml
kubectl logs -n jafra-system daemonset/jafra-agent -f
```

The DaemonSet defaults to `log-only`. After the analyzer is up:

```bash
kubectl set env daemonset/jafra-agent -n jafra-system JAFRA_MODE=grpc
```

The DaemonSet mounts `/var/lib/jafra/recordings` read-write so it can delete
closed files. It runs only on Linux nodes, including Kind control-plane
nodes via a taint toleration.

## Intentional limitations

- Agent acknowledgements are process-local. Restarting the agent before
  deletion can re-upload; the analyzer answers `DUPLICATE` from its PVC
  identity store.
- Watcher events only wake a rescan; finalization is decided from the JFR
  header, never from a single inotify event.
- Continuous JFR writes generate many inotify events. The 1024-deep wake
  channel can overflow, which forces a full rescan. The
  `incomplete_chunks` metric currently counts every growing-tail observation,
  not unique incomplete chunks.
- `hostPath` is required for the node-local demonstration.
- Recording directories are mode `0777` so the unprivileged agent can delete
  files the JVM wrote as another UID. That is demonstration-only.
