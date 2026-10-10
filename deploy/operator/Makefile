# The module stands alone: it is not part of the repository's go.work.
export GOWORK = off

IMG ?= ghcr.io/queen-mq/queen-operator:dev
CONTROLLER_GEN = go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.22.0

.PHONY: all generate manifests test build docker-build

all: generate manifests test build

# The deep-copy code of the API types.
generate:
	$(CONTROLLER_GEN) object paths=./api/...

# The CRD and the ClusterRole from the markers in the code, and install.yaml:
# everything a cluster needs, in one file.
manifests:
	$(CONTROLLER_GEN) crd rbac:roleName=queen-operator paths=./... output:crd:dir=config/crd output:rbac:dir=config/rbac
	cat config/crd/queenmq.com_queenclusters.yaml config/rbac/role.yaml > install.yaml
	printf -- '---\n' >> install.yaml
	cat config/manager/manager.yaml >> install.yaml

test:
	go vet ./...
	go test ./...

build:
	go build -o bin/queen-operator ./cmd

docker-build:
	docker build -t $(IMG) .
