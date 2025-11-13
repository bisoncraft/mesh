#!/bin/sh

set +x

go version

# run tests
env GORACE="halt_on_error=1" go test -count 1 -timeout 20s -race -cover ./...

echo "-----------------------------"
echo "Tests completed successfully!"