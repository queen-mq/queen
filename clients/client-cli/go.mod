module github.com/smartpricing/queen/clients/client-cli

go 1.24.0

require (
	github.com/charmbracelet/lipgloss v0.13.1
	github.com/dustin/go-humanize v1.0.1
	github.com/jackc/pgx/v5 v5.8.0
	github.com/mattn/go-isatty v0.0.20
	github.com/smartpricing/queen/clients/client-go v0.0.0
	github.com/spf13/cobra v1.8.1
	github.com/zalando/go-keyring v0.2.5
	golang.org/x/term v0.35.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/alessio/shellescape v1.4.1 // indirect
	github.com/aymanbagabas/go-osc52/v2 v2.0.1 // indirect
	github.com/charmbracelet/x/ansi v0.3.2 // indirect
	github.com/danieljoos/wincred v1.2.0 // indirect
	github.com/godbus/dbus/v5 v5.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/muesli/termenv v0.15.2 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace github.com/smartpricing/queen/clients/client-go => ../client-go
