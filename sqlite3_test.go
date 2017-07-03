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
package sqlite3

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pborman/uuid"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
)

const (
	DB_DRIVER   = "sqlite3"
	DB_FILE     = "./test.db"
	DB_TABLE    = "testtable"
	DB_UP_DDL   = "CREATE TABLE `" + DB_TABLE + "` (`id` VARCHAR(128) PRIMARY KEY,`etag` VARCHAR(128),`updated` VARCHAR(128),`created` VARCHAR(128),`f1` VARCHAR(128),`f2` INTEGER);"
	DB_DOWN_DDL = "DROP TABLE `" + DB_TABLE + "`;"
)

var i1, _ = item("foo", 1)
var i2, _ = item("bar", 2)

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
	p["created"] = "2006-01-02 15:04:05.99999999 -0700 MST"
	p["f1"] = f1
	p["f2"] = f2
	return resource.NewItem(p)
}

func callGetSelect(h *Handler, q query.Query, s string, v schema.Validator, offset, limit int) (string, error) {
	l := resource.NewLookup()
	l.AddQuery(q)
	l.SetSort(s, v)
	return getSelect(h, l, offset, limit)
}

// TestModel tests the insert functionality.
func TestModel(t *testing.T) {
	Convey("Get a handler should work", t, func() {
		h, err := handler()
		So(err, ShouldBeNil)
		h.session.Exec(DB_DOWN_DDL)
		_, err = h.session.Exec(DB_UP_DDL)
		So(err, ShouldBeNil)

		Convey(`Insert operation should return nil upon success`, func() {
			var l = []*resource.Item{i1, i2}
			result := h.Insert(context.Background(), l)
			So(result, ShouldBeNil)

			Convey("Find should return an item list", func() {
				l := resource.NewLookup()
				Convey("Found item should match i1", func() {
					q := query.Query{query.Equal{Field: "f1", Value: "foo"}}
					l.AddQuery(q)
					result, err := h.Find(context.Background(), l, 0, 10)
					So(err, ShouldBeNil)
					So(result.Total, ShouldEqual, 1)
					So(result.Offset, ShouldEqual, 0)
					So(len(result.Items), ShouldEqual, 1)
					So(result.Items[0].ID, ShouldEqual, i1.ID)
					So(result.Items[0].ETag, ShouldEqual, i1.ETag)
					So(result.Items[0].Payload["id"], ShouldEqual, i1.Payload["id"])
					So(result.Items[0].Payload["f1"], ShouldEqual, i1.Payload["f1"])
					So(result.Items[0].Payload["f2"], ShouldEqual, i1.Payload["f2"])
					So(fmt.Sprintf("%v", result.Items[0].Updated), ShouldEqual, fmt.Sprintf("%v", i1.Updated))
					//So(result.Items[0].Payload, ShouldResemble, i2.Payload) // fails, existing PR on assertions may fix
				})
				Convey("Found item should match i2", func() {
					q := query.Query{query.Equal{Field: "f1", Value: "bar"}}
					l.AddQuery(q)
					result, err := h.Find(context.Background(), l, 0, 10)
					So(err, ShouldBeNil)
					So(result.Total, ShouldEqual, 1)
					So(result.Offset, ShouldEqual, 0)
					So(result.Limit, ShouldEqual, 10)
					So(len(result.Items), ShouldEqual, 1)
					So(result.Items[0].ID, ShouldEqual, i2.ID)
					So(result.Items[0].ETag, ShouldEqual, i2.ETag)
					So(result.Items[0].Payload["id"], ShouldEqual, i2.Payload["id"])
					So(result.Items[0].Payload["f1"], ShouldEqual, i2.Payload["f1"])
					So(result.Items[0].Payload["f2"], ShouldEqual, i2.Payload["f2"])
					So(fmt.Sprintf("%v", result.Items[0].Updated), ShouldEqual, fmt.Sprintf("%v", i2.Updated))
					//So(result.Items[0].Payload, ShouldResemble, i2.Payload) // fails, existing PR on assertions may fix
				})
			})

			Convey(`Successful delete operations should return nil`, func() {
				result = h.Delete(context.Background(), i1)
				So(result, ShouldBeNil)
				result = h.Delete(context.Background(), i2)
				So(result, ShouldBeNil)

				Convey(`Attempt to delete missing id should return resource.ErrNotFound`, func() {
					result = h.Delete(context.Background(), i2)
					So(result, ShouldEqual, resource.ErrNotFound)
				})
			})

			Convey(`Successful clear operations should return the number of affected rows`, func() {
				l := resource.NewLookup()
				q := query.Query{query.Or{query.Equal{Field: "f1", Value: "foo"}, query.Equal{Field: "f1", Value: "bar"}}}
				l.AddQuery(q)
				result, err := h.Clear(context.Background(), l)
				So(err, ShouldBeNil)
				So(result, ShouldEqual, 2)
				result, err = h.Clear(context.Background(), l)
				So(err, ShouldBeNil)
				So(result, ShouldEqual, 0)

				Convey(`Attempt to clear missing rows should return 0`, func() {
					result, err = h.Clear(context.Background(), l)
					So(err, ShouldBeNil)
					So(result, ShouldEqual, 0)
				})
			})

			Convey("SELECT statements should be correct", func() {
				q := query.Query{query.Equal{Field: "f1", Value: "foo"}}
				v := schema.Schema{Fields: schema.Fields{
					"id": schema.IDField,
					"f1": schema.Field{Sortable: true},
				}}
				s, err := callGetSelect(h, q, "-f1,f1", v, 0, -1)
				So(err, ShouldBeNil)
				So(s, ShouldEqual, "SELECT * FROM "+h.tableName+" WHERE f1 LIKE 'foo' ESCAPE '\\' ORDER BY f1 DESC,f1;")
			})

			Convey("SELECT statements with pagination should be correct", func() {
				q := query.Query{query.Equal{Field: "f1", Value: "foo"}}
				v := schema.Schema{Fields: schema.Fields{
					"id": schema.IDField,
					"f1": schema.Field{Sortable: true},
				}}
				s, err := callGetSelect(h, q, "-f1,f1", v, 0, 10)
				So(err, ShouldBeNil)
				So(s, ShouldEqual, "SELECT * FROM "+h.tableName+" WHERE f1 LIKE 'foo' ESCAPE '\\' ORDER BY f1 DESC,f1 LIMIT 10;")
			})

			Convey("UPDATE statements should be correct", func() {
				var u, upd, etag, id string
				var testItem, _ = item("foo", 1)
				delete(testItem.Payload, "created")
				var err error
				id, err = valueToString(testItem.ID)
				So(err, ShouldBeNil)
				etag, err = valueToString(testItem.ETag)
				So(err, ShouldBeNil)
				upd, err = valueToString(testItem.Updated)
				So(err, ShouldBeNil)

				u, err = getUpdate(h, testItem, testItem)
				So(err, ShouldBeNil)
				So(u, ShouldEqual, "UPDATE OR ROLLBACK "+h.tableName+" SET etag="+etag+",updated="+upd+",f1='foo',f2=1 WHERE id="+id+" AND etag="+etag+";")
			})

			Convey("DELETE statements should be correct", func() {
				q := query.Query{query.Equal{Field: "f1", Value: "foo"}}
				So(err, ShouldBeNil)
				s, err := callGetDelete(h, q)
				So(err, ShouldBeNil)
				So(s, ShouldEqual, "DELETE FROM "+h.tableName+" WHERE f1 LIKE 'foo' ESCAPE '\\';")
			})

		})

		//Reset(func() {
		//	_, err = h.session.Exec(DB_DOWN_DDL)
		//	So(err, ShouldBeNil)
		//})
	})
}
