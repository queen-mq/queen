module crossbench

go 1.25.0

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/rabbitmq/amqp091-go v1.13.0
	github.com/smartpricing/queen/clients/client-go v0.0.0
	github.com/twmb/franz-go v1.21.5
	github.com/twmb/franz-go/pkg/kadm v1.18.0
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.51.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/text v0.37.0 // indirect
)

replace github.com/smartpricing/queen/clients/client-go => ../../clients/client-go
