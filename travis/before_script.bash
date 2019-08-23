#!/usr/bin/env bash
set -eux

psql -U postgres -c 'create database pglogrepl;'
psql -U postgres -c "create user pglogrepl with replication password 'secret';"
