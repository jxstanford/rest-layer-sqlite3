package sqlite3

import (
	"testing"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"
	. "github.com/smartystreets/goconvey/convey"
)

func callGetQuery(q query.Query) (string, error) {
	l := resource.NewLookup()
	l.AddQuery(q)
	return getQuery(l)
}

func callGetSort(s string, v schema.Validator) string {
	l := resource.NewLookup()
	l.SetSort(s, v)
	return getSort(l)
}

func callGetDelete(h *Handler, q query.Query) (string, error) {
	l := resource.NewLookup()
	l.AddQuery(q)
	return getDelete(h, l)
}

func TestLookups(t *testing.T) {
	Convey("Queries should do the right thing", t, func() {

		// equality and type handling
		s, err := callGetQuery(query.Query{query.Equal{Field: "f1", Value: "foo"}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 LIKE 'foo' ESCAPE '\\'")

		// _ is not interpreted as a single character wildcard
		s, err = callGetQuery(query.Query{query.Equal{Field: "f1", Value: "foo_bar"}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 LIKE 'foo\\_bar' ESCAPE '\\'")

		// * is interpreted as a multicharacter wildcard
		s, err = callGetQuery(query.Query{query.Equal{Field: "f1", Value: "foo*bar"}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 LIKE 'foo%bar' ESCAPE '\\'")

		s, err = callGetQuery(query.Query{query.Equal{Field: "id", Value: 10}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "id IS 10")

		s, err = callGetQuery(query.Query{query.Equal{Field: "id", Value: true}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "id IS true")

		s, err = callGetQuery(query.Query{query.Equal{Field: "id", Value: 10.01}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "id IS 10.01")

		var l = []string{"a", "b"}
		_, err = callGetQuery(query.Query{query.Equal{Field: "id", Value: l}})
		So(err, ShouldEqual, resource.ErrNotImplemented)

		// inequality
		s, err = callGetQuery(query.Query{query.NotEqual{Field: "f1", Value: "foo"}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 NOT LIKE 'foo' ESCAPE '\\'")

		// _ is not interpreted as a single character wildcard
		s, err = callGetQuery(query.Query{query.NotEqual{Field: "f1", Value: "foo_bar"}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 NOT LIKE 'foo\\_bar' ESCAPE '\\'")

		// * is interpreted as a multicharacter wildcard
		s, err = callGetQuery(query.Query{query.NotEqual{Field: "f1", Value: "foo*bar"}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 NOT LIKE 'foo%bar' ESCAPE '\\'")

		s, err = callGetQuery(query.Query{query.GreaterThan{Field: "f1", Value: 1}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 > 1")

		s, err = callGetQuery(query.Query{query.GreaterOrEqual{Field: "f1", Value: 1}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 >= 1")

		s, err = callGetQuery(query.Query{query.LowerThan{Field: "f1", Value: 1}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 < 1")

		s, err = callGetQuery(query.Query{query.LowerOrEqual{Field: "f1", Value: 1}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "f1 <= 1")

		// membership
		s, err = callGetQuery(query.Query{query.In{Field: "id", Values: []query.Value{"a", "b"}}})
		So(err, ShouldEqual, nil)
		So(s, ShouldEqual, "id IN ('a','b')")

		s, err = callGetQuery(query.Query{query.NotIn{Field: "id", Values: []query.Value{"a", "b"}}})
		So(err, ShouldEqual, nil)
		So(s, ShouldEqual, "id NOT IN ('a','b')")

		// simple logical operators
		s, err = callGetQuery(query.Query{query.And{query.Equal{Field: "id", Value: 10}, query.Equal{Field: "f1", Value: "foo"}}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "(id IS 10 AND f1 LIKE 'foo' ESCAPE '\\')")
		s, err = callGetQuery(query.Query{query.Or{query.Equal{Field: "id", Value: 10}, query.Equal{Field: "f1", Value: "foo"}}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "(id IS 10 OR f1 LIKE 'foo' ESCAPE '\\')")

		// compound logical operators
		s, err = callGetQuery(query.Query{
			query.And{
				query.Equal{Field: "id", Value: 10},
				query.Equal{Field: "f1", Value: "foo"},
				query.Or{
					query.Equal{Field: "id", Value: 10},
					query.Equal{Field: "f1", Value: "foo"}}}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "(id IS 10 AND f1 LIKE 'foo' ESCAPE '\\' AND (id IS 10 OR f1 LIKE 'foo' ESCAPE '\\'))")

		s, err = callGetQuery(query.Query{
			query.Or{
				query.Equal{Field: "id", Value: 10},
				query.Equal{Field: "f1", Value: "foo"},
				query.And{
					query.Equal{Field: "id", Value: 10},
					query.Equal{Field: "f1", Value: "foo"}}}})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, "(id IS 10 OR f1 LIKE 'foo' ESCAPE '\\' OR (id IS 10 AND f1 LIKE 'foo' ESCAPE '\\'))")
	})

	Convey("Sorts should do the right thing", t, func() {
		var s string
		v := schema.Schema{Fields: schema.Fields{
			"id": schema.IDField,
			"f":  schema.Field{Sortable: true},
		}}

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
