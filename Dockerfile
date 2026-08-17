FROM rust:1.85-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY contracts ./contracts
COPY jafra-agent ./jafra-agent
WORKDIR /workspace/jafra-agent
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /workspace/jafra-agent/target/release/jafra-agent /usr/local/bin/jafra-agent
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/jafra-agent"]
