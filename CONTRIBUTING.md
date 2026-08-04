# Contributing to Queen MQ

Thanks for your interest in Queen!

- **Bug reports & feature requests** — open an issue using the [issue templates](https://github.com/queen-mq/queen/issues/new/choose). For bugs, include the server version, how you run it (Docker, Helm, bare binary), and the client/SDK you use.
- **Code contributions** — everything you need to build, run, and test every component (the Rust broker, SQL schema, dashboard, proxy, and all six client SDKs) is in **[Building and developing](https://queenmq.com/internals/contributing)**. Start there; it gets you from clone to a running broker in the right build order.
- **Pull requests** — target `master`, keep changes focused, and run the test suite of the component you touched. `test/run.sh` runs every client suite against every topology; see [Testing](https://queenmq.com/internals/contributing/testing).
- **Security issues** — please do **not** open a public issue; see [SECURITY.md](SECURITY.md).
- **Documentation** — all docs live in `webdoc/` and are published at [queenmq.com](https://queenmq.com/). How the site is built and what it is allowed to claim is in [Writing the documentation](https://queenmq.com/internals/contributing/docs).

By contributing you agree that your contributions are licensed under the [Apache 2.0 License](LICENSE.md).
