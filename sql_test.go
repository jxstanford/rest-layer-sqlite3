// Package sql tests assume SQLlite3 installed, and the following table
// definition installed:
//
// CREATE TABLE `testtable` (
//     `id` VARCHAR(128) PRIMARY KEY,
//     `etag` VARCHAR(128),
//     `updated` DATE NULL,
//    `f1` VARCHAR(128),
//    `f2` INTEGER
// );
package sql

import (
	"testing"

	"database/sql"
	_ "github.com/mattn/go-sqlite3"

	"golang.org/x/net/context"

	"code.google.com/p/go-uuid/uuid"

	log "github.com/Sirupsen/logrus"
	"github.com/rs/rest-layer/resource"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	DB_DRIVER = "sqlite3"
	DB_FILE = "./test.db"
	DB_TABLE = "testtable"
)

// handler returns a new handler with the database and table information,
// or an error.
func handler() (*Handler, error) {
	db, err := sql.Open(DB_DRIVER, DB_FILE)
	if err != nil {
		return nil, err
	}
	return NewHandler(db, DB_TABLE), nil
}

func item(f1 string, f2 int) (*resource.Item, error) {
	p := make(map[string]interface{})
	p["id"] = uuid.New()
	p["f1"] = f1
	p["f2"] = f2
	return resource.NewItem(p)
}

// TestInsert tests the insert functionality.
func TestInsert(t *testing.T) {
	Convey("Insert operation tests", t, func() {
		h, err := handler()
		So(err, ShouldBeNil)
		defer h.session.Close()

		i1, err := item("f1", 1)
		if err != nil {
			log.WithError(err).Warn("Failed to create test item1.")
		}
		i2, err := item("f2", 2)
		if err != nil {
			log.WithError(err).Warn("Failed to create test item2.")
		}

		var l = []*resource.Item {i1, i2}
		result := h.Insert(context.Background(), l)
		Convey(`Insert operation should return nil`, func() {
			So(result, ShouldBeNil)
		})
	})
}

func TestDelete(t *testing.T) {
	Convey("Delete should return an error", t, func() {
		db, err := sql.Open("sqlite3", "./test.db")
		h := NewHandler(db, "testtable")

		i, err := item("f1", 1)
		if err != nil {
			log.Warn("Failed to create test item.")
		}

		result := h.Delete(context.Background(), i)


		Convey(`Delete operation should return ErrNotFound`, func() {
			So(result, ShouldEqual, resource.ErrNotFound)
		})
	})
}
