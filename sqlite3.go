// Package sql is a REST Layer resource storage handler for databases supported via
// drivers for database/sql. It implements the Storer interface defined in
// rest-layer/resource/storage.go.
package sqlite3

import (
	"database/sql"
	"fmt"

	"golang.org/x/net/context"

	"github.com/rs/rest-layer/resource"

	log "github.com/Sirupsen/logrus"
	"time"
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
func (h *Handler) Find(ctx context.Context, lookup *resource.Lookup, page, perPage int) (*resource.ItemList, error) {
	var q string // query string
	var err error
	var rows *sql.Rows // query result
	var cols []string  // column names
	raw := []map[string]interface{}{} // holds the raw results as a map of columns:values

	// build a paginated select statement based
	q, err = getSelect(h, lookup, page, perPage)
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
	return newItemList(raw, page)

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

// Update replace an item in the backend store by a new version. The ResourceHandler must
// ensure that the original item exists in the database and has the same Etag field.
// This check should be performed atomically. If the original item is not
// found, a resource.ErrNotFound must be returned. If the etags don't match, a
// resource.ErrConflict must be returned.
//
// The item payload must be stored together with the etag and the updated field.
// The item.ID and the payload["id"] is guaranteed to be identical, so there's not need
// to store both.
//
// If the storage of the data is not immediate, the method must listen for cancellation
// on the passed ctx. If the operation is stopped due to context cancellation, the
// function must return the result of the ctx.Err() method.
func (h *Handler) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {

	return resource.ErrNotFound
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

	// query for record with the same id, and return ErrNotFound if we don't.
	var etag string
	err = h.session.QueryRow(
		fmt.Sprintf("SELECT etag FROM %s WHERE id = '%s'", h.tableName, item.ID)).Scan(&etag)
	if err != nil {
		switch {
		case err.Error() == SQL_NOTFOUND_ERR:
			txPtr.Rollback()
			return resource.ErrNotFound
		default:
			txPtr.Rollback()
			log.WithFields(log.Fields{
				"id":    item.ID,
				"error": err,
			}).Warn("Error querying record to delete.")
			return err
		}
	}

	// compare the etags to ensure that someone else hasn't scooped us.
	if etag != item.ETag {
		txPtr.Rollback()
		log.WithFields(log.Fields{
			"id":    item.ID,
			"error": err,
		}).Warn("ETag of record to delete does not match the one supplied.")
		return resource.ErrConflict
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
func (h *Handler) Clear(ctx context.Context, lookup *resource.Lookup) (int64, error) {

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
	return ra, nil
}

// getSelect returns a SQL SELECT statement that represents the Lookup data
func getSelect(h *Handler, l *resource.Lookup, page, perPage int) (string, error) {
	str := "SELECT * FROM " + h.tableName + " WHERE "
	q, err := getQuery(l)
	if err != nil {
		log.WithField("error", err).Warn("Error building query for select statement.")
		return "", err
	}
	str += q
	str += " ORDER BY " + getSort(l)

	if perPage >= 0 {
		str += fmt.Sprintf(" LIMIT %d", perPage)
		str += fmt.Sprintf(" OFFSET %d", (page-1)*perPage)
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
				"key":    k,
				"error": err,
			}).Warn("Error converting payload value to string.", )
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

// newItemList creates a list of resource.Item from a SQL result row slice
func newItemList(rows []map[string]interface{}, page int) (*resource.ItemList, error) {

	l := &resource.ItemList{Page: page, Total: len(rows), Items: []*resource.Item{}}
	for _, r := range rows {
		i, err := newItem(r)
		if err != nil {
			log.WithField("error", err).Warn("Error creating an Item from a row.")
			return nil, err
		}
		l.Items = append(l.Items, i)
	}
	return l, nil
}

// newItem creates resource.Item from a SQL result row
func newItem(row map[string]interface{}) (*resource.Item, error) {
	// Add the id back (we use the same map hoping the mongoItem won't be stored back)
	id := row["id"]
	etag := row["etag"]
	updated := row["updated"]
	delete(row, "etag")
	delete(row, "updated")

	t, err := time.Parse("2006-01-02 15:04:05.99999999 -0700 MST", updated.(string))
	if err != nil {
		log.WithField("error", err).Warn("Error parsing updated.")
		return nil, err
	}
	return &resource.Item{
		ID:      id,
		ETag:    etag.(string),
		Updated: t,
		Payload: row,
	}, nil
}
