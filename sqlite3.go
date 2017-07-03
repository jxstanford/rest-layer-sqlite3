// Package sql is a REST Layer resource storage handler for databases supported via
// drivers for database/sql. It implements the Storer interface defined in
// rest-layer/resource/storage.go.
package sqlite3

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"
)

const (
	SQL_NOTFOUND_ERR = "sql: no rows in result set"
)

// Handler contains the session and table information for a SQL DB.
type Handler struct {
	session   *sql.DB
	tableName string
}

// NewHandler creates an new SQL DB session handler.
func NewHandler(s *sql.DB, tableName string) *Handler {
	return &Handler{
		session:   s,
		tableName: tableName,
	}
}

// Find searches for items in the backend store matching the lookup argument.
// If no items are found, an empty list is returned with no error. If a query
// operation is not implemented, a resource.ErrNotImplemented is returned.
func (h *Handler) Find(ctx context.Context, lookup *resource.Lookup, offset, limit int) (*resource.ItemList, error) {
	var q string // query string
	var err error
	var rows *sql.Rows                // query result
	var cols []string                 // column names
	raw := []map[string]interface{}{} // holds the raw results as a map of columns:values

	// build a paginated select statement based
	q, err = getSelect(h, lookup, offset, limit)
	if err != nil {
		log.WithField("error", err).Warn("Error getting the select statement.")
		return nil, err
	}

	// execute the DB query, get the results
	rows, err = h.session.Query(q)
	if err != nil {
		log.WithField("error", err).Warn("Error querying the DB.")
		return nil, err
	}
	defer rows.Close()

	cols, err = rows.Columns()
	if err != nil {
		log.WithField("error", err).Warn("Error getting columns.")
		return nil, err
	}

	for rows.Next() {
		rowMap := make(map[string]interface{})       // col:val map for a row
		rowVals := make([]interface{}, len(cols))    // values for a row
		rowValPtrs := make([]interface{}, len(cols)) // pointers to row values used by Scan

		// create the pointers to the row value elements
		for i, _ := range cols {
			rowValPtrs[i] = &rowVals[i]
		}

		// scan into the pointer slice (and set the values)
		err := rows.Scan(rowValPtrs...)
		if err != nil {
			log.WithField("error", err).Warn("Error scanning a row.")
			return nil, err
		}

		// convert byte arrays to strings
		for i, v := range rowVals {
			b, ok := v.([]byte)
			if ok {
				v = string(b)
			}
			rowMap[cols[i]] = v
		}

		// add the row to the intermediate data structure
		raw = append(raw, rowMap)
	}

	// check for any errors during row iteration
	err = rows.Err()
	if err != nil {
		log.WithField("error", err).Warn("Error during row iteration.")
		return nil, err
	}

	// return a *resource.ItemList or an error
	return newItemList(raw, offset, limit)

}

// Insert stores new items in the backend store. If any of the items already exist,
// no item should be inserted and a resource.ErrConflict must be returned. The insertion
// of the items is performed atomically.
func (h *Handler) Insert(ctx context.Context, items []*resource.Item) error {

	// begin a database transaction
	txPtr, err := h.session.Begin()
	if err != nil {
		log.WithField("error", err).Warn("Error starting insert transaction.")
		return err
	}

	// construct and execute an insert statement for each item provided.  If anything
	// fails, rollback the transaction and return.
	for _, i := range items {
		s, err := getInsert(h, i)
		if err != nil {
			txPtr.Rollback()
			log.WithField("error", err).Warn("Error creating insert statement.")
			return err
		}
		_, err = h.session.Exec(s)
		if err != nil {
			txPtr.Rollback()
			log.WithField("error", err).Warn("Error executing insert statement.")
			return err
		}
	}
	// inserts all succeeded, commit the transaction.
	txPtr.Commit()
	return nil
}

// Update replaces an item in the backend store with a new version. If the original
// item is not found, a resource.ErrNotFound is returned. If the etags don't match, a
// resource.ErrConflict is returned.
func (h *Handler) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {

	// begin a database transaction
	txPtr, err := h.session.Begin()
	if err != nil {
		log.WithField("error", err).Warn("Error starting update transaction.")
		return err
	}

	// get the original item
	l := resource.NewLookup()
	q := query.Query{query.Equal{Field: "id", Value: original.ID}}
	l.AddQuery(q)
	s, err := getSelect(h, l, 1, 1)
	if err != nil {
		txPtr.Rollback()
		log.WithField("error", err).Warn("Error constructing select to retreive original record.")
		return err
	}

	err = compareEtags(h, original.ID, original.ETag)
	if err != nil {
		txPtr.Rollback()
		log.WithField("error", err).Warn("Error comparing ETags.")
		return err
	}

	s, err = getUpdate(h, item, original)
	if err != nil {
		txPtr.Rollback()
		log.WithField("error", err).Warn("Error creating update statement.")
		return err
	}
	_, err = h.session.Exec(s)
	if err != nil {
		txPtr.Rollback()
		log.WithField("error", err).Warn("Error executing update statement.")
		return err
	}

	// update succeeded, commit the transaction.
	txPtr.Commit()
	return nil
}

// Delete deletes the provided item by its ID. The Etag of the item stored in the
// backend store must match the Etag of the provided item or a resource.ErrConflict
// must be returned. This check should be performed atomically.
//
// If the provided item were not present in the backend store, a resource.ErrNotFound
// must be returned.
//
// If the removal of the data is not immediate, the method must listen for cancellation
// on the passed ctx. If the operation is stopped due to context cancellation, the
// function must return the result of the ctx.Err() method.
func (h *Handler) Delete(ctx context.Context, item *resource.Item) error {

	// begin a transaction
	txPtr, err := h.session.Begin()
	if err != nil {
		log.WithFields(log.Fields{
			"id":    item.ID,
			"error": err,
		}).Warn("Error starting delete transaction.")
		return err
	}

	err = compareEtags(h, item.ID, item.ETag)
	if err != nil {
		txPtr.Rollback()
		log.WithField("error", err).Warn("Error comparing ETags.")
		return err
	}

	// prepare and execute the delete statement, then finish the transaction
	s := fmt.Sprintf("DELETE FROM %s WHERE id = '%s'", h.tableName, item.ID)
	stmt, err := h.session.Prepare(s)
	if err != nil {
		log.WithFields(log.Fields{
			"id":    item.ID,
			"error": err,
		}).Warn("Error preparing delete statement.")
		txPtr.Rollback()
		return err
	}

	_, err = stmt.Exec()
	if err != nil {
		log.WithFields(log.Fields{
			"id":    item.ID,
			"error": err,
		}).Warn("Error executing delete statement.")
		txPtr.Rollback()
		return err
	}

	txPtr.Commit()
	return nil
}

// Clear removes all items matching the lookup and returns the number of items
// removed as the first value.  If a query operation is not implemented
// by the storage handler, a resource.ErrNotImplemented is returned.
func (h *Handler) Clear(ctx context.Context, lookup *resource.Lookup) (int, error) {

	// construct the delete statement from the lookup data
	s, err := getDelete(h, lookup)
	if err != nil {
		log.WithField("error", err).Warn("Error building delete statement for clear.")
		return -1, err // should only be ErrNotImplemented
	}
	result, err := h.session.Exec(s)
	if err != nil {
		log.WithField("error", err).Warn("Error executing delete statement for clear.")
		return -1, err
	}
	ra, err := result.RowsAffected()
	if err != nil {
		log.WithField("error", err).Warn("Error getting row count for clear.")
		return -1, nil
	}
	return int(ra), nil
}

// getSelect returns a SQL SELECT statement that represents the Lookup data
func getSelect(h *Handler, l *resource.Lookup, offset, limit int) (string, error) {
	str := "SELECT * FROM " + h.tableName
	q, err := getQuery(l)
	if err != nil {
		log.WithField("error", err).Warn("Error building query for select statement.")
		return "", err
	}
	if q != "" {
		str += " WHERE " + q
	}
	if l.Sort() != nil {
		str += " ORDER BY " + getSort(l)
	}

	if limit >= 0 {
		str += fmt.Sprintf(" LIMIT %d", limit)
	}
	if offset > 0 {
		str += fmt.Sprintf(" OFFSET %d", offset)
	}
	str += ";"
	return str, nil
}

// getDelete returns a SQL DELETE statement that represents the Lookup data
func getDelete(h *Handler, l *resource.Lookup) (string, error) {
	str := "DELETE FROM " + h.tableName + " WHERE "
	q, err := getQuery(l)
	if err != nil {
		log.WithField("error", err).Warn("Error building query for delete statement.")
		return "", err
	}
	str += q + ";"
	return str, nil
}

// getInsert returns a SQL INSERT statement constructed from the Item data
func getInsert(h *Handler, i *resource.Item) (string, error) {
	var etag, upd string
	var err error

	etag, err = valueToString(i.ETag)
	if err != nil {
		log.WithField("error", err).Warn("Error converting ETag to string.")
		return "", resource.ErrNotImplemented
	}
	upd, err = valueToString(i.Updated)
	if err != nil {
		log.WithField("error", err).Warn("Error converting Updated to string.")
		return "", resource.ErrNotImplemented
	}
	a := fmt.Sprintf("INSERT INTO %s(etag,updated,", h.tableName)
	z := fmt.Sprintf("VALUES(%s,%s,", etag, upd)
	for k, v := range i.Payload {
		var val string
		a += k + ","
		val, err = valueToString(v)
		if err != nil {
			log.WithFields(log.Fields{
				"key":   k,
				"error": err,
			}).Warn("Error converting payload value to string.")
			return "", resource.ErrNotImplemented
		}
		z += val + ","
	}
	// remove trailing commas
	a = a[:len(a)-1] + ")"
	z = z[:len(z)-1] + ")"

	result := fmt.Sprintf("%s %s;", a, z)
	return result, nil
}

// getUpdate returns a SQL INSERT statement constructed from the Item data
func getUpdate(h *Handler, i *resource.Item, o *resource.Item) (string, error) {
	var id, oEtag, iEtag, upd string
	var err error

	id, err = valueToString(o.ID)
	if err != nil {
		log.WithField("error", err).Warn("Error converting ID to string.")
		return "", resource.ErrNotImplemented
	}
	oEtag, err = valueToString(o.ETag)
	if err != nil {
		log.WithField("error", err).Warn("Error converting original ETag to string.")
		return "", resource.ErrNotImplemented
	}
	iEtag, err = valueToString(i.ETag)
	if err != nil {
		log.WithField("error", err).Warn("Error converting new ETag to string.")
		return "", resource.ErrNotImplemented
	}
	upd, err = valueToString(i.Updated)
	if err != nil {
		log.WithField("error", err).Warn("Error converting Updated to string.")
		return "", resource.ErrNotImplemented
	}
	a := fmt.Sprintf("UPDATE OR ROLLBACK %s SET etag=%s,updated=%s,", h.tableName, iEtag, upd)
	z := fmt.Sprintf("WHERE id=%s AND etag=%s;", id, oEtag)
	for k, v := range i.Payload {
		if k != "id" {
			var val string
			val, err = valueToString(v)
			if err != nil {
				log.WithFields(log.Fields{
					"key":   k,
					"error": err,
				}).Warn("Error converting payload value to string.")
				return "", resource.ErrNotImplemented
			}
			a += fmt.Sprintf("%s=%s,", k, val)
		}

	}
	// remove trailing comma
	a = a[:len(a)-1]

	result := fmt.Sprintf("%s %s", a, z)
	return result, nil
}

// newItemList creates a list of resource.Item from a SQL result row slice
func newItemList(rows []map[string]interface{}, offset, limit int) (*resource.ItemList, error) {
	items := make([]*resource.Item, len(rows))
	l := &resource.ItemList{Offset: offset, Limit: limit, Total: len(rows), Items: items}
	for i, r := range rows {
		item, err := newItem(r)
		if err != nil {
			log.WithField("error", err).Warn("Error creating an Item from a row.")
			return nil, err
		}
		items[i] = item
	}
	return l, nil
}

// newItem creates resource.Item from a SQL result row
func newItem(row map[string]interface{}) (*resource.Item, error) {
	// Add the id back (we use the same map hoping the mongoItem won't be stored back)
	id := row["id"]
	etag := row["etag"]
	created := row["created"]
	updated := row["updated"]
	delete(row, "etag")
	delete(row, "updated")

	ct, err := time.Parse("2006-01-02 15:04:05.99999999 -0700 MST", created.(string))
	if err != nil {
		log.WithField("error", err).Warn("Error parsing updated.")
		return nil, err
	}
	row["created"] = ct

	tu, err := time.Parse("2006-01-02 15:04:05.99999999 -0700 MST", updated.(string))
	if err != nil {
		log.WithField("error", err).Warn("Error parsing updated.")
		return nil, err
	}
	return &resource.Item{
		ID:      id,
		ETag:    etag.(string),
		Updated: tu,
		Payload: row,
	}, nil
}

func compareEtags(h *Handler, id, origEtag interface{}) error {
	// query for record with the same id, and return ErrNotFound if we don't find one.
	var etag string
	var err error
	err = h.session.QueryRow(
		fmt.Sprintf("SELECT etag FROM %s WHERE id='%v'", h.tableName, id)).Scan(&etag)
	if err != nil {
		switch {
		case err.Error() == SQL_NOTFOUND_ERR:
			return resource.ErrNotFound
		default:
			log.WithFields(log.Fields{
				"id":    id,
				"error": err,
			}).Warn("Error querying record to delete.")
			return err
		}
	}

	// compare the etags to ensure that someone else hasn't scooped us.
	if etag != origEtag {
		log.WithFields(log.Fields{
			"id":    id,
			"error": err,
		}).Warn("ETag of record does not match the one supplied.")
		return resource.ErrConflict
	}

	return nil
}
