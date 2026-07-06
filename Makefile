PROTO_DIR   := proto
PROTO_FILE  := $(PROTO_DIR)/kronos.proto
GEN_DIR     := $(PROTO_DIR)
PROTOC_OPTS := --go_out=$(PROTO_DIR) --go_opt=paths=source_relative \
               --go-grpc_out=$(PROTO_DIR) --go-grpc_opt=paths=source_relative \
               -I $(PROTO_DIR)

.PHONY: proto build tidy chaos

proto:
	protoc $(PROTOC_OPTS) $(PROTO_FILE)
	@echo "Generated gRPC stubs → $(GEN_DIR)"

build:
	go build -o bin/scheduler ./scheduler
	go build -o bin/worker    ./worker
	go build -o bin/client    ./client

chaos:
	cd chaos && go test -v -timeout 180s .

tidy:
	cd proto     && go mod tidy
	cd scheduler && go mod tidy
	cd worker    && go mod tidy
	cd client    && go mod tidy
