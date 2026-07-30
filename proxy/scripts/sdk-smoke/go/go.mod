module github.com/smartpricing/queen/proxy/scripts/sdk-smoke/go

go 1.24.0

require github.com/smartpricing/queen/clients/client-go v0.0.0

require github.com/google/uuid v1.6.0 // indirect

// The published client is consumed straight from the checkout -- no publish
// step, no version pin: the smoke has to exercise the client in this tree.
replace github.com/smartpricing/queen/clients/client-go => ../../../../clients/client-go
