FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY src ./src
COPY sql ./sql
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=build /src/target/release/queen-seg /queen-seg
EXPOSE 6632
ENV PORT=6632
ENTRYPOINT ["/queen-seg"]
