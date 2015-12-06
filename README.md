# REST Layer SQLite3 Backend

[![godoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/jxstanford/rest-layer-sqlite3) [![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/jxstanford/rest-layer-sqlite3/master/LICENSE) [![build](https://img.shields.io/travis/jxstanford/rest-layer-sqlite3.svg?style=flat)](https://travis-ci.org/jxstanford/rest-layer-sqlite3)

This [REST Layer](https://github.com/rs/rest-layer) resource storage backend stores data in a SQLite3 database  using [database/sql](https://godoc.org/database/sql) and [go-sqlite3](https://godoc.org/github.com/mattn/go-sqlite3).

## Usage

This backend assumes that you have created the database schema in SQLite3 to match your REST API schema.  For a reasonably complete example, look at example_test.go.  To run the example, copy it to a new location, rename it to `example.go`, change the package name to `main`, and change the name of the `Example` function to `main`, finally, `go run example.go`.

## Caveats

This backend does not currently implement the following features of the interface:

* array fields
* dict fields
* $exists filter (not applicable to a DB with predefined schema)
* field selection 
* field aliasing 
* embedding
* field parameters

