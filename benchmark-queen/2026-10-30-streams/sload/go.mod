module sload

go 1.24.0

require github.com/smartpricing/queen/clients/client-go/v2 v2.0.0

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
)

replace github.com/smartpricing/queen/clients/client-go/v2 => ../../../clients/client-go
