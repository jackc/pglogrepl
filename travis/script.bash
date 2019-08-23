#!/usr/bin/env bash
set -eux

go test -v -race ./...
