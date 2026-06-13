.PHONY: run build clean test test-ratelimiter test-consistent-hashing bench-consistent-hashing dist-consistent-hashing

run:
	go run ./cmd/server/

build:
	go build -o bin/server ./cmd/server/

clean:
	rm -rf bin/

test:
	go test ./...

# Rate limiter package
test-ratelimiter:
	go test ./ratelimiter/

# Consistent hashing package
test-consistent-hashing:
	go test ./consistent-hashing/

bench-consistent-hashing:
	go test ./consistent-hashing/ -bench . -benchmem

# Print the load-distribution table (keys-per-server vs. virtual-node count)
dist-consistent-hashing:
	go test ./consistent-hashing/ -run TestLoadDistribution -v
