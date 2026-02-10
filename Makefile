DB_PATH         = ./ducklake_meta.sqlite
MIGRATIONS_DIR  = db/migrations

.PHONY: migrate-up migrate-down migrate-status sqlc new-migration build test vet

migrate-up:
	goose -dir $(MIGRATIONS_DIR) sqlite3 $(DB_PATH) up

migrate-down:
	goose -dir $(MIGRATIONS_DIR) sqlite3 $(DB_PATH) down

migrate-status:
	goose -dir $(MIGRATIONS_DIR) sqlite3 $(DB_PATH) status

new-migration:
	goose -dir $(MIGRATIONS_DIR) create $(NAME) sql

sqlc:
	sqlc generate

build:
	go build ./...

test:
	go test -race ./...

vet:
	go vet ./...
