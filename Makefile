.PHONY: default
default: build

.PHONY: build
build: clean clean-data generate
	go build -v -o bin/dbadger-cli ./cmd/example-cli

.PHONY: clean
clean:
	rm -rf bin/*

.PHONY: clean-data
clean-data:
	rm -rf .data/*

.PHONY: generate
generate:
	protoc \
			--go_out=. --go_opt=paths=source_relative \
			--go-grpc_out=. --go-grpc_opt=paths=source_relative \
			internal/rpc/service.proto

.PHONY: lint
lint:
	golangci-lint version
	golangci-lint run -v

.PHONY: test
test:
	go test -v -race -count=1 ./...

.PHONY: cover
cover:
	go test -coverpkg=github.com/w1ck3dg0ph3r/dbadger/... -covermode=set -coverprofile=coverage.txt ./...
	go tool cover -html=coverage.txt -o coverage.html
	go tool cover -func=coverage.txt

.PHONY: profile
profile:
	go test -count=1 -run=^$$ -bench=.* -outputdir=.prof -cpuprofile=cpu.out -memprofile=mem.out -blockprofile=block.out ./test

.PHONY: trace
trace:
	go test -count=1 -trace=.prof/trace.out ./test

.PHONY: godoc
godoc:
	godoc -index -http=:6060

.PHONY: start-example
start-example:
	go run ./cmd/example-cli --inmem --bind 127.0.0.1:7001 --bootstrap