SHELL = /bin/bash
IMAGE_TAG := zeppelin-sqlproxy

build:
	CGO_ENABLED=0 go build -ldflags '-extldflags "-static"' -o output/main ./cmd/root.go
clean:
	rm -rf output
docker:
	DOCKER_BUILDKIT=1 docker build -f ./Dockerfile -t $(IMAGE_TAG) .
run: docker
	docker run --rm $(IMAGE_TAG)
test:
	CGO_ENABLED=0 go build -ldflags '-extldflags "-static"' -o output/tester ./cmd/tester.go
	./output/tester