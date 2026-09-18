// Run-log checker, PLAN_RAFT.md §13.7.
//
// Standard library only: the checker has to run wherever a log lands (the
// Linux VM of §13.6 is rsynced without a module cache), so it vendors nothing
// and depends on nothing. `porcupine` for the KV linearizability check (§13.7)
// is the one planned exception and is NOT added until that check is written —
// see checks.go, CheckKVLinearizable.
//
// The repo's go.work lists only clients/client-cli and clients/client-go, so
// every go command here runs with GOWORK=off (see ../README.md).
module queen/test/raft/checker

go 1.24
