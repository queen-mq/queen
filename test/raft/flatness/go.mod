// Flatness harness, PLAN_RAFT.md §13.6 (G-3, I8).
//
// Standard library only: the preloader runs on the Linux VM, which is rsynced
// without a module cache, and the comparator has to run anywhere a pair of
// RESULTS files lands.
//
// Go rather than Python because the preloader is the one tool here that has to
// keep a broker busy: 300 million messages at a useful rate is not a Python
// loop. The comparator lives in the same module so that the format it reads is
// the format the runner writes, in one place.
//
// The repo's go.work lists only clients/client-cli and clients/client-go, so
// every go command here runs with GOWORK=off (see ../README.md).
module queen/test/raft/flatness

go 1.24
