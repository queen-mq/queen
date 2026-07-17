# go-hotpath-spike broker image — for a like-for-like docker-stats comparison
# against smartnessai/queen-mq (C++) on the same host + same Postgres.
FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV GOWORK=off
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/go-hotpath ./...

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/go-hotpath /go-hotpath
EXPOSE 6632
# Default port matches the C++ broker so the same clients/harness hit it unchanged.
ENV PORT=6632
ENTRYPOINT ["/go-hotpath"]
