package sqlite3

import (
	"testing"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	. "github.com/smartystreets/goconvey/convey"
)

func callGetQuery(q schema.Query) (string, error) {
	l := resource.NewLookup()
	l.AddQuery(q)
	return getQuery(l)
}

func callGetSort(s string, v schema.Validator) string {
	l := resource.NewLookup()
	l.SetSort(s, v)
	return getSort(l)
}

func callGetDelete(h *Handler, q schema.Query) (string, error) {
	l := resource.NewLookup()
	l.AddQuery(q)
	return getDelete(h, l)
}

func TestLookups(t *testing.T) {
	Convey("Queries should do the right thing", t, func() {

		// equality and type handling
		s, err := callGetQuery(schema.Query{schema.Equal{Field: "f1", Value: "foo"}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 IS 'foo'")

		s, err = callGetQuery(schema.Query{schema.Equal{Field: "id", Value: 10}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "id IS 10")

		s, err = callGetQuery(schema.Query{schema.Equal{Field: "id", Value: true}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "id IS true")

		s, err = callGetQuery(schema.Query{schema.Equal{Field: "id", Value: 10.01}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "id IS 10.01")

		var l = []string{"a", "b"}
		_, err = callGetQuery(schema.Query{schema.Equal{Field: "id", Value: l}})
		So(err, ShouldEqual, resource.ErrNotImplemented)

		// inequality
		s, err = callGetQuery(schema.Query{schema.NotEqual{Field: "f1", Value: "foo"}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 IS NOT 'foo'")

		s, err = callGetQuery(schema.Query{schema.GreaterThan{Field: "f1", Value: 1}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 > 1")

		s, err = callGetQuery(schema.Query{schema.GreaterOrEqual{Field: "f1", Value: 1}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 >= 1")

		s, err = callGetQuery(schema.Query{schema.LowerThan{Field: "f1", Value: 1}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 < 1")

		s, err = callGetQuery(schema.Query{schema.LowerOrEqual{Field: "f1", Value: 1}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 <= 1")

		// membership
		s, err = callGetQuery(schema.Query{schema.In{Field: "id", Values: []schema.Value{"a", "b"}}})
		So(err, ShouldEqual, nil)
		So(s, ShouldEqual, "id IN ('a','b')")

		s, err = callGetQuery(schema.Query{schema.NotIn{Field: "id", Values: []schema.Value{"a", "b"}}})
		So(err, ShouldEqual, nil)
		So(s, ShouldEqual, "id NOT IN ('a','b')")

		// simple logical operators
		s, err = callGetQuery(schema.Query{schema.And{schema.Equal{Field: "id", Value: 10}, schema.Equal{Field: "f1", Value: "foo"}}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "(id IS 10 AND f1 IS 'foo')")
		s, err = callGetQuery(schema.Query{schema.Or{schema.Equal{Field: "id", Value: 10}, schema.Equal{Field: "f1", Value: "foo"}}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "(id IS 10 OR f1 IS 'foo')")

		// compound logical operators
		s, err = callGetQuery(schema.Query{
			schema.And{
				schema.Equal{Field: "id", Value: 10},
				schema.Equal{Field: "f1", Value: "foo"},
				schema.Or{
					schema.Equal{Field: "id", Value: 10},
					schema.Equal{Field: "f1", Value: "foo"}}}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "(id IS 10 AND f1 IS 'foo' AND (id IS 10 OR f1 IS 'foo'))")

		s, err = callGetQuery(schema.Query{
			schema.Or{
				schema.Equal{Field: "id", Value: 10},
				schema.Equal{Field: "f1", Value: "foo"},
				schema.And{
					schema.Equal{Field: "id", Value: 10},
					schema.Equal{Field: "f1", Value: "foo"}}}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "(id IS 10 OR f1 IS 'foo' OR (id IS 10 AND f1 IS 'foo'))")
	})

	Convey("Sorts should do the right thing", t, func() {
		var s string
		v := schema.Schema{"id": schema.IDField, "f": schema.Field{Sortable: true}}

		s = callGetSort("", v)
		So(s, ShouldEqual, "id")

		s = callGetSort("id", v)
		So(s, ShouldEqual, "id")

		s = callGetSort("f", v)
		So(s, ShouldEqual, "f")

		s = callGetSort("-f", v)
		So(s, ShouldEqual, "f DESC")

		s = callGetSort("f,-f", v)
		So(s, ShouldEqual, "f,f DESC")
	})
}
