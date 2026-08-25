# Compile rustc on the builder's native arch, then cross-link for TARGETPLATFORM.
# qemu-user SIGSEGVs rustc when --platform linux/amd64 is used on arm64 hosts.
FROM --platform=$BUILDPLATFORM rust:1.85-bookworm AS build

ARG BUILDPLATFORM
ARG TARGETPLATFORM

RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        gcc \
        libc6-dev \
    && if [ "${TARGETPLATFORM}" != "${BUILDPLATFORM}" ]; then \
         case "${TARGETPLATFORM}" in \
           linux/amd64) \
             apt-get install -y --no-install-recommends gcc-x86-64-linux-gnu libc6-dev-amd64-cross ;; \
           linux/arm64|linux/arm64/v8) \
             apt-get install -y --no-install-recommends gcc-aarch64-linux-gnu libc6-dev-arm64-cross ;; \
           *) echo "unsupported TARGETPLATFORM=${TARGETPLATFORM}" >&2; exit 1 ;; \
         esac; \
       fi \
    && rm -rf /var/lib/apt/lists/*

RUN case "${TARGETPLATFORM}" in \
      linux/amd64) echo x86_64-unknown-linux-gnu >/tmp/rust-target ;; \
      linux/arm64|linux/arm64/v8) echo aarch64-unknown-linux-gnu >/tmp/rust-target ;; \
      *) echo "unsupported TARGETPLATFORM=${TARGETPLATFORM}" >&2; exit 1 ;; \
    esac \
 && rustup target add "$(cat /tmp/rust-target)"

WORKDIR /workspace
COPY contracts ./contracts
COPY jafra-agent ./jafra-agent
WORKDIR /workspace/jafra-agent

RUN RUST_TARGET="$(cat /tmp/rust-target)" \
 && case "${RUST_TARGET}" in \
      x86_64-unknown-linux-gnu) \
        export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc ;; \
      aarch64-unknown-linux-gnu) \
        export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc ;; \
    esac \
 && if [ "${TARGETPLATFORM}" = "${BUILDPLATFORM}" ]; then \
      unset CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER \
            CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER; \
    fi \
 && cargo build --release --target "${RUST_TARGET}" \
 && mkdir -p /out \
 && cp "target/${RUST_TARGET}/release/jafra-agent" /out/jafra-agent

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/jafra-agent /usr/local/bin/jafra-agent
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/jafra-agent"]
