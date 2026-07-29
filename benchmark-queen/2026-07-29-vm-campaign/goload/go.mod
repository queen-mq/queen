module goload

go 1.24.0

require github.com/smartpricing/queen/clients/client-go v0.0.0

require github.com/google/uuid v1.6.0 // indirect

replace github.com/smartpricing/queen/clients/client-go => ../../../clients/client-go
