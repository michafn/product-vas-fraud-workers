# Fraud Sync

## Environment variables

See `.env` file

## Run locally

Use `$ go run worker.go`

## Run in a Docker container

1. Build a Docker image:
  - `$ docker build --pull -t fraudsync:latest .`

2. Run our worker in a Docker container:
  - `$ docker run --rm --env-file .env fraudsync:latest`
