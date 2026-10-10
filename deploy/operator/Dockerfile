# The Queen operator: one static Go binary.
#
#   docker build -t ghcr.io/queen-mq/queen-operator:dev deploy/operator
#
# The build context is this directory: the module stands alone and is not
# part of the repository's go.work.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api api
COPY cmd cmd
COPY internal internal
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOWORK=off \
    go build -trimpath -ldflags="-s -w" -o /out/queen-operator ./cmd

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/queen-operator /queen-operator
USER 65532:65532
ENTRYPOINT ["/queen-operator"]
