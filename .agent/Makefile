.PHONY: check lint smoke lint-actions act-ci

check:
	./check.sh

lint:
	./lint.sh

smoke:
	./smoke.sh

lint-actions:
	@echo "Installing actionlint"
	go install github.com/rhysd/actionlint/cmd/actionlint@latest
	@echo "Running actionlint"
	"$(shell go env GOPATH)/bin/actionlint"

act-ci:
	./act-ci.sh
