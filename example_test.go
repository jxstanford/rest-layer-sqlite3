package sqlite3_test

import (
    "os"
	"log"
	"net/http"
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/cors"
	"github.com/jxstanford/rest-layer-sqlite3"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/rest"
	"github.com/rs/rest-layer/schema"
)

const (
	DB_DRIVER   = "sqlite3"
	DB_FILE     = "./example.db"
	USER_TABLE    = "users"
	POST_TABLE = "posts"
    ENABLE_FK = "PRAGMA foreign_keys = ON;"
	USERS_UP_DDL   = "CREATE TABLE `" + USER_TABLE + "` (`id` VARCHAR(128) PRIMARY KEY,`etag` VARCHAR(128),`updated` VARCHAR(128),`created` VARCHAR(128), `name` VARCHAR(150));"
	POSTS_UP_DDL   = "CREATE TABLE `" + POST_TABLE + "` (`id` VARCHAR(128) PRIMARY KEY,`etag` VARCHAR(128),`updated` VARCHAR(128), `created` VARCHAR(128), `user` VARCHAR(128) REFERENCES users(id) ON DELETE CASCADE, `public` INTEGER, `title` VARCHAR(150), `body` VARCHAR(100000));"
	USERS_DN_DDL   = "DROP TABLE `" + USER_TABLE + "`;"
	POSTS_DN_DDL   = "DROP TABLE `" + POST_TABLE + "`;"
)

var (
	user = schema.Schema{
		"id":      schema.IDField,
		"created": schema.CreatedField,
		"updated": schema.UpdatedField,
		"name": schema.Field{
			Required:   true,
			Filterable: true,
			Sortable:   true,
			Validator: &schema.String{
				MaxLen: 150,
			},
		},
	}

	// Define a post resource schema
	post = schema.Schema{
		"id":      schema.IDField,
		"created": schema.CreatedField,
		"updated": schema.UpdatedField,
		"user": schema.Field{
			Required:   true,
			Filterable: true,
			Validator: &schema.Reference{
				Path: "users",
			},
		},
		"public": schema.Field{
			Filterable: true,
			Validator:  &schema.Bool{},
		},
		"title": schema.Field{
			Required: true,
			Validator: &schema.String{
				MaxLen: 150,
			},
		},
		"body": schema.Field{
			Validator: &schema.String{
				MaxLen: 100000,
			},
		},
	}
)

// handler returns a new handler with the database and table information,
// or an error.


func Example() {
    dbDn()
    // get a database connection and set up the tables.
	db, err := sql.Open(DB_DRIVER, DB_FILE)
	if err != nil {
		log.Fatal(err)
	}
    dbUp(db)
    //defer dbDn(db)

	index := resource.NewIndex()

	users := index.Bind("users", resource.New(user, sqlite3.NewHandler(db, USER_TABLE), resource.Conf{
		AllowedModes: resource.ReadWrite,
	}))

	users.Bind("posts", "user", resource.New(post, sqlite3.NewHandler(db, POST_TABLE), resource.Conf{
		AllowedModes: resource.ReadWrite,
	}))

	api, err := rest.NewHandler(index)
	if err != nil {
		log.Fatalf("Invalid API configuration: %s", err)
	}

	http.Handle("/", cors.New(cors.Options{OptionsPassthrough: true}).Handler(api))

	log.Print("Serving API on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}

func dbDn() {
    err := os.Remove(DB_FILE)
    if err != nil {
        //log.Warn(err)
    }
}

func dbUp(db *sql.DB) {
    var err error
    _, err = db.Exec(ENABLE_FK)
    if err != nil {
        log.Fatal(err)
    }
    _, err = db.Exec(USERS_UP_DDL) 
    if err != nil {
        log.Fatal(err)
    }
    _, err = db.Exec(POSTS_UP_DDL) 
    if err != nil {
        log.Fatal(err)
    }
}


