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

```go
import "github.com/rs/rest-layer-sqlite3"
```

Create a mgo master session:

```go
session, err := mgo.Dial(url)
```

Create a resource storage handler with a given DB/collection:

```go
s := mongo.NewHandler(session, "the_db", "the_collection")
```

Use this handler with a resource:

```go
index.Bind("foo", resource.NewResource(foo, s, resource.DefaultConf)
```

You may want to create a many mongo handlers as you have resources as long as you want each resources in a different collection. You can share the same `mgo` session across all you handlers.

### Object ID

This package also provides a REST Layer [schema.Validator](https://godoc.org/github.com/rs/rest-layer/schema#Validator) for MongoDB ObjectIDs. This validator ensures proper binary serialization of the Object ID in the database for space efficiency.

You may reference this validator using [mongo.ObjectID](https://godoc.org/github.com/rs/rest-layer-mongo#ObjectID) as [schema.Field](https://godoc.org/github.com/rs/rest-layer/schema#Field).

A `mongo.NewObjectID` field hook and `mongo.ObjectIDField` helper are also provided.
