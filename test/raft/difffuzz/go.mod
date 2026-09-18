// Differential fuzzer, PLAN_RAFT.md §13.4.
//
// Standard library only, on purpose: this harness has to build on a machine
// with no module proxy (the Linux VM of §13.6 is rsynced without a module
// cache), so it vendors nothing and depends on nothing.
//
// The repo's root go.work lists only clients/client-cli and clients/client-go,
// so every go command here runs with GOWORK=off (see README.md).
module queen/test/raft/difffuzz

go 1.24
