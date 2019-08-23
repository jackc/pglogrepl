#!/usr/bin/env bash
set -eux

if [ "${PGVERSION-}" != "" ]
then
  sudo apt-get remove -y --purge postgresql libpq-dev libpq5 postgresql-client-common postgresql-common
  sudo rm -rf /var/lib/postgresql
  wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
  sudo sh -c "echo deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main $PGVERSION >> /etc/apt/sources.list.d/postgresql.list"
  sudo apt-get update -qq
  sudo apt-get -y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::="--force-confnew" install postgresql-$PGVERSION postgresql-server-dev-$PGVERSION postgresql-contrib-$PGVERSION
  sudo chmod 777 /etc/postgresql/$PGVERSION/main/pg_hba.conf
  echo "local     all         postgres                    trust"    >  /etc/postgresql/$PGVERSION/main/pg_hba.conf
  echo "host      replication pglogrepl 127.0.0.1/32      md5"      >> /etc/postgresql/$PGVERSION/main/pg_hba.conf
  sudo chmod 777 /etc/postgresql/$PGVERSION/main/postgresql.conf
  echo "wal_level='logical'"     >> /etc/postgresql/$PGVERSION/main/postgresql.conf
  echo "max_wal_senders=5"       >> /etc/postgresql/$PGVERSION/main/postgresql.conf
  echo "max_replication_slots=5" >> /etc/postgresql/$PGVERSION/main/postgresql.conf
  sudo /etc/init.d/postgresql restart
fi
