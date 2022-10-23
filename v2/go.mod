module github.com/wttw/pglogrepl/v2

go 1.13

require (
	github.com/jackc/pgconn v1.6.5-0.20200823013804-5db484908cf7
	github.com/jackc/pgio v1.0.0
	github.com/jackc/pglogrepl v0.0.0-00010101000000-000000000000
	github.com/jackc/pglogrepl/v2 v2.0.0-00010101000000-000000000000
	github.com/jackc/pgproto3/v2 v2.0.4
	github.com/jackc/pgtype v0.0.0-20190828014616-a8802b16cc59
	github.com/stretchr/testify v1.5.1
	golang.org/x/xerrors v0.0.0-20191011141410-1b5146add898
)

replace github.com/jackc/pglogrepl => /Users/steve/dev/pglogrepl

replace github.com/jackc/pglogrepl/v2 => /Users/steve/dev/pglogrepl/v2
