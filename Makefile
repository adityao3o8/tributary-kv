.PHONY: gen build test run clean tidy

# Regenerate protobuf/gRPC Go code from proto/*.proto.
gen:
	buf generate

# Build both binaries into ./bin.
build:
	go build -o bin/quorumd ./cmd/quorumd
	go build -o bin/quorumctl ./cmd/quorumctl

test:
	go test ./...

# Start a single node (override with: make run LISTEN=:7001 ID=node-2).
LISTEN ?= :7000
ID ?= node-1
run: build
	./bin/quorumd --id $(ID) --listen $(LISTEN)

tidy:
	go mod tidy

clean:
	rm -rf bin
