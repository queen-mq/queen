FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY build.rs ./
COPY server.json ./
COPY src ./src
COPY sql ./sql
COPY webapp ./webapp
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=build /src/target/release/queen /queen
EXPOSE 6632
ENV PORT=6632
ENTRYPOINT ["/queen"]
