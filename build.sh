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

usage() {
    cat <<'EOF'
usage: ./build.sh [broker|proxy|all] [options]

  --push              push to the registry instead of loading into the local daemon
  --multiarch         build linux/amd64 + linux/arm64 (implies --push; needs a
                      docker-container builder, see the error text if missing)
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

# Multi-arch manifests need the docker-container driver; the default `docker`
# driver silently cannot produce them, so fail here with the fix rather than
# letting buildx error halfway through a Rust release build.
if [ "$MULTIARCH" = true ]; then
    DRIVER=$(docker buildx inspect 2>/dev/null | awk -F': *' '/^Driver:/{print $2; exit}')
    if [ "$DRIVER" != "docker-container" ]; then
        cat >&2 <<EOF
build.sh: --multiarch needs a docker-container builder (current driver: ${DRIVER:-none}).

    docker buildx create --name queen --driver docker-container --use --bootstrap
EOF
        exit 1
    fi
    PLATFORMS="linux/amd64,linux/arm64"
else
    # Host arch only. Emulating the other one costs a full Rust release build
    # under QEMU/Rosetta — fine for a release, painful for iterating.
    PLATFORMS="linux/$(docker version --format '{{.Server.Arch}}')"
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
