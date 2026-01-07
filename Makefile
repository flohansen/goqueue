################################################################################

GO   ?= go
SQLC ?= sqlc

################################################################################

.PHONY: generate
generate:
	$(SQLC) generate

.PHONY: test
test:
	$(GO) test ./... -cover -v

.PHONY: test-it
test-it:
	$(GO) test -tags=integration ./tests/integration/... -v
