.DEFAULT_GOAL := test

.PHONY: test run-api run-worker run-scheduler migrate bootstrap ui-dev ui-build
test:
	go test ./...

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

run-scheduler:
	go run ./cmd/scheduler

migrate:
	go run ./cmd/migrate

bootstrap:
	go run ./cmd/bootstrap

ui-dev:
	cd frontend && npm run dev

ui-build:
	cd frontend && npm run build
