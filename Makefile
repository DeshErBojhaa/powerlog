compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

TAG ?=0.0.1

build-docker:
	docker build -t github.com/desherbojhaa/powerlog:$(TAG) .

lint-docker:
	docker run --rm -i ghcr.io/hadolint/hadolint < Dockerfile