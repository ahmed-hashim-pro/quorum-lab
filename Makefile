# Thin wrappers over go, plus the targets that regenerate what the README shows.
GO ?= go

.PHONY: build test check hunt mutants demo clean

build:
	$(GO) build -o quorumlab ./cmd/quorumlab

test:
	$(GO) vet ./...
	$(GO) test ./...

# The two claims at full size: the correct build survives 20,000 hostile
# seeds, and every deliberate bug is caught.
hunt: build
	./quorumlab hunt --seeds 20000

mutants: build
	./quorumlab mutants --seeds 3000

check: test hunt mutants

# Re-render the README timelines and re-record docs/demo.gif from the real CLI.
# The GIF needs Docker (VHS). The binary is static, so it runs in that image.
demo:
	CGO_ENABLED=0 $(GO) build -o quorumlab ./cmd/quorumlab
	./quorumlab run --seed 2 --svg docs/timeline.svg --from 0 --to 6000 > /dev/null
	./quorumlab run --seed 258 --bug commit-previous-term --svg docs/figure8.svg --from 3000 --to 4900 > /dev/null || true
	docker run --rm -u $$(id -u):$$(id -g) -e HOME=/tmp -v $(CURDIR):/vhs \
	  -v $(CURDIR):/opt/quorumlab:ro ghcr.io/charmbracelet/vhs:v0.10.0 docs/demo.tape

clean:
	rm -f quorumlab
