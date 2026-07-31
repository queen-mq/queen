module github.com/smartpricing/queen/examples/full/go

go 1.24.0

require github.com/smartpricing/queen/clients/client-go v0.0.0

replace github.com/smartpricing/queen/clients/client-go => ../../../clients/client-go

require github.com/google/uuid v1.6.0 // indirect
