SERVER  := server
CLIENT  := client

GO      ?= go
SOURCES := $(shell find cmd internal -name '*.go' -not -name '*_test.go') go.mod

all: $(SERVER) $(CLIENT)

$(SERVER): $(SOURCES)
	$(GO) build -o $@ ./cmd/server

$(CLIENT): $(SOURCES)
	$(GO) build -o $@ ./cmd/client

test:
	$(GO) test ./...

clean:
	rm -f $(SERVER) $(CLIENT)

re:
	$(MAKE) clean
	$(MAKE) all

.PHONY: all test clean re
