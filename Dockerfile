# The image only packages a manager binary cross-compiled on the host by
# `make build-image-binaries`, so builds reuse the host's Go build cache. Compiling inside the
# build container started from a cold cache on every CI run (~7 minutes per architecture): BuildKit
# cache mounts are not exported with the layer cache.
#
# There is deliberately no "# syntax=" line: the built-in frontend covers this file, and an external
# one is fetched from Docker Hub on every build (a Docker Hub auth outage failed CI). Nothing here
# pulls from Docker Hub.
FROM gcr.io/distroless/static:nonroot
ARG TARGETOS
ARG TARGETARCH
WORKDIR /
COPY dist/manager-${TARGETOS}-${TARGETARCH} /manager
USER 65532:65532

ENTRYPOINT ["/manager"]
