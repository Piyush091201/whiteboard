# Convenience targets. On Windows without `make`, run the underlying `go`
# commands directly (see README "Development" section).

.PHONY: run build test test-race lint fmt fmt-check vet tidy

run:
	go run ./cmd/server

build:
	go build ./...

test:
	go test ./...

# The headline target: all tests under the race detector, the way CI runs them.
test-race:
	go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

fmt-check:
	gofmt -l .

lint:
	golangci-lint run

tidy:
	go mod tidy
