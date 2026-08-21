#!/usr/bin/env bash
#
# Build and publish the Queen container images to GHCR (or any OCI registry).
#
#   ./build.sh                      # build the broker for the host arch, keep it local
#   ./build.sh all                  # build both images locally
#   ./build.sh proxy --push         # build + publish the proxy
#   ./build.sh all --push --multiarch --latest
#
# Both images build from the repo root: the proxy crate embeds the broker's
# dashboard from ../server/webapp/dist, so its context cannot be proxy/.
#
# Publishing needs a token with the `write:packages` scope. The GitHub CLI is
# the least painful source of one (GHCR does not accept fine-grained PATs):
#
#   gh auth refresh -h github.com -s write:packages
#   gh auth token | docker login ghcr.io -u <your-github-user> --password-stdin
#
# A newly published package is PRIVATE until you flip it once, by hand, at
# https://github.com/orgs/queen-mq/packages -> the package -> Package settings.

set -euo pipefail

cd "$(dirname "$0")"

REGISTRY="${QUEEN_REGISTRY:-ghcr.io/queen-mq}"
SOURCE_URL="https://github.com/queen-mq/queen"

TARGET="broker"
PUSH=false
MULTIARCH=false
TAG_LATEST=false
TAG_OVERRIDE=""
PLATFORM_OVERRIDE=""

usage() {
    cat <<'EOF'
usage: ./build.sh [broker|proxy|all] [options]

  --push              push to the registry instead of loading into the local daemon
  --multiarch         build linux/amd64 + linux/arm64 (implies --push; needs a
                      docker-container builder, see the error text if missing)
  --platform P        build for P instead of the host arch, e.g. linux/amd64.
                      Cross-building runs the whole Rust compile under QEMU, so
                      it is slow; it is also the only way to get a deployable
                      image for an amd64 cluster from an arm64 machine.
  --amd64             shorthand for --platform linux/amd64
  --latest            also tag :latest
  --registry REG      registry/namespace (default: ghcr.io/queen-mq,
                      override with $QUEEN_REGISTRY)
  --tag TAG           use TAG instead of the version from the manifest
  -h, --help          this text

Image names: broker -> $REGISTRY/queen, proxy -> $REGISTRY/queen-proxy
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        broker|proxy|all) TARGET="$1" ;;
        --push)       PUSH=true ;;
        --multiarch)  MULTIARCH=true; PUSH=true ;;
        --platform)   PLATFORM_OVERRIDE="$2"; shift ;;
        --amd64)      PLATFORM_OVERRIDE="linux/amd64" ;;
        --latest)     TAG_LATEST=true ;;
        --registry)   REGISTRY="$2"; shift ;;
        --tag)        TAG_OVERRIDE="$2"; shift ;;
        -h|--help)    usage; exit 0 ;;
        *) echo "build.sh: unknown argument '$1'" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

command -v jq >/dev/null || { echo "build.sh: jq is required" >&2; exit 1; }
command -v docker >/dev/null || { echo "build.sh: docker is required" >&2; exit 1; }

HOST_PLATFORM="linux/$(docker version --format '{{.Server.Arch}}')"

if [ "$MULTIARCH" = true ] && [ -n "$PLATFORM_OVERRIDE" ]; then
    echo "build.sh: --multiarch and --platform are mutually exclusive" >&2
    exit 2
fi

if [ "$MULTIARCH" = true ]; then
    PLATFORMS="linux/amd64,linux/arm64"
elif [ -n "$PLATFORM_OVERRIDE" ]; then
    PLATFORMS="$PLATFORM_OVERRIDE"
else
    # Host arch only — the safe default for iterating, and USELESS for a cluster
    # whose nodes are a different architecture: the pod dies with `exec format
    # error`. Use --amd64 (or --multiarch) to produce something deployable.
    PLATFORMS="$HOST_PLATFORM"
fi

# A multi-platform manifest needs the docker-container driver; the default
# `docker` driver silently cannot produce one, so fail here with the fix rather
# than letting buildx error halfway through a Rust release build.
if [ "$MULTIARCH" = true ]; then
    DRIVER=$(docker buildx inspect 2>/dev/null | awk -F': *' '/^Driver:/{print $2; exit}')
    if [ "$DRIVER" != "docker-container" ]; then
        cat >&2 <<EOF
build.sh: --multiarch needs a docker-container builder (current driver: ${DRIVER:-none}).

    docker buildx create --name queen --driver docker-container --use --bootstrap
EOF
        exit 1
    fi
fi

# A single foreign platform does NOT need that driver, but it does need QEMU
# binfmt registered, otherwise the first RUN in the foreign stage dies with
# "exec format error" — the same symptom as deploying the wrong arch, which
# makes it worth naming here.
if [ "$PLATFORMS" != "$HOST_PLATFORM" ] && [ "$MULTIARCH" != true ]; then
    # Capture, THEN grep. Piping straight into `grep -q` under `pipefail` is a
    # race: grep exits on the first match, inspect catches SIGPIPE if it is
    # still writing, and the pipeline reports failure on a builder that
    # advertised the platform perfectly well. Bit two real builds before being
    # caught (flaky only when inspect's output landed slower than grep's exit).
    BUILDER_PLATFORMS=$(docker buildx inspect --bootstrap 2>/dev/null || true)
    if ! printf '%s' "$BUILDER_PLATFORMS" | grep -q "$PLATFORMS"; then
        cat >&2 <<EOF
build.sh: this builder does not advertise $PLATFORMS (host is $HOST_PLATFORM).

Register QEMU emulation once, then retry:

    docker run --privileged --rm tonistiigi/binfmt --install all

Docker Desktop ships this already; a plain dockerd usually does not.
EOF
        exit 1
    fi
    echo "==> cross-building $PLATFORMS on $HOST_PLATFORM under emulation — this is slow"
fi

COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)

# One image: $1 name, $2 dockerfile, $3 version, then any extra --build-arg pairs.
build_image() {
    local name="$1" dockerfile="$2" version="$3"; shift 3
    local image="$REGISTRY/$name"
    local tag="${TAG_OVERRIDE:-$version}"

    local args=(
        --file "$dockerfile"
        --platform "$PLATFORMS"
        --tag "$image:$tag"
        # Links the package to the repo on GHCR — without it the package is
        # orphaned at org level, with no README and no source link. Actions sets
        # this up implicitly; a manual push does not.
        --label "org.opencontainers.image.source=$SOURCE_URL"
        --label "org.opencontainers.image.revision=$COMMIT"
        --label "org.opencontainers.image.version=$tag"
        --label "org.opencontainers.image.licenses=Apache-2.0"
        "$@"
    )

    [ "$TAG_LATEST" = true ] && args+=(--tag "$image:latest")

    if [ "$PUSH" = true ]; then
        args+=(--push)
    else
        # --load cannot handle a multi-platform result; that combination is
        # already ruled out above since --multiarch forces PUSH=true.
        args+=(--load)
    fi

    echo "==> $image:$tag  [$PLATFORMS]$([ "$PUSH" = true ] && echo ' push' || echo ' local')"
    DOCKER_BUILDKIT=1 docker buildx build "${args[@]}" .
}

build_broker() {
    local version
    version=$(jq -r '.version' server/server.json)
    # queenctl reports this string from inside the container, so it has to match
    # the tag the image is published under.
    build_image queen ./Dockerfile "$version" \
        --build-arg "QUEENCTL_VERSION=${TAG_OVERRIDE:-$version}" \
        --build-arg "QUEENCTL_COMMIT=$COMMIT"
}

build_proxy() {
    local version
    version=$(awk -F'"' '/^\[package\]/{p=1} p && /^version[[:space:]]*=/{print $2; exit}' \
        proxy/Cargo.toml)
    build_image queen-proxy ./proxy/Dockerfile "$version"
}

case "$TARGET" in
    broker) build_broker ;;
    proxy)  build_proxy ;;
    all)    build_broker; build_proxy ;;
esac

echo "==> done"
