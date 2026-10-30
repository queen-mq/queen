# Contributing to Queen MQ

Thanks for your interest in Queen!

- **Bug reports & feature requests** — open an issue using the [issue templates](https://github.com/queen-mq/queen/issues/new/choose). For bugs, include the server version, how you run it (Docker, Helm, bare binary), and the client/SDK you use.
- **Code contributions** — we prefer that you open an issue and we work on it.
- **Pull requests** — target `master`, keep changes focused, and run the test suite of the component you touched. `test/run.sh` runs every client suite against every topology; see [Testing](https://queenmq.com/architecture/contributing/).
- **Sign off every commit** — `git commit -s` adds a `Signed-off-by:` line with your name and email. It certifies the [Developer Certificate of Origin](https://developercertificate.org): you wrote the change, or have the right to submit it under the project's license. A check on every pull request refuses a commit without one; `git rebase --signoff master` signs the commits of a branch you already made.
- **Test language** — write test names, comments, fixture output, and assertion messages in English.
- **Security issues** — please do **not** open a public issue; see [SECURITY.md](SECURITY.md).
- **Documentation** — all docs live in `webdoc/` and are published at [queenmq.com](https://queenmq.com/). How the site is built and what it is allowed to claim is in [Writing the documentation](https://queenmq.com/architecture/contributing/).

By contributing you agree that your contributions are licensed under the [Apache 2.0 License](LICENSE.md).

How the project is run is in [GOVERNANCE.md](GOVERNANCE.md), and who runs it in [MAINTAINERS.md](MAINTAINERS.md). Everyone who takes part follows the [Code of Conduct](CODE_OF_CONDUCT.md).
