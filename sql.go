// Package sql is a REST Layer resource storage handler for databases supported via
// drivers for database/sql. It implements the Storer interface defined in
// rest-layer/resource/storage.go.
package sql

import (
	"database/sql"

	"golang.org/x/net/context"

	"github.com/rs/rest-layer/resource"

	log "github.com/Sirupsen/logrus"
	"fmt"
)

// Handler handles resource storage in a PostgreSQL table.
type Handler struct {
	session   *sql.DB
	tableName string
}

// NewHandler creates an new PostgreSQL handler
func NewHandler(s *sql.DB, tableName string) *Handler {
	return &Handler{
		session: s,
		tableName: tableName,
	}
}

// Find searches for items in the backend store matching the lookup argument. The
// pagination argument must be respected. If no items are found, an empty list
// should be returned with no error.
//
// If the total number of item can't be easily computed, ItemList.Total should
// be set to -1. The requested page should be set to ItemList.Page.
//
// The whole lookup query must be treated. If a query operation is not implemented
// by the storage handler, a resource.ErrNotImplemented must be returned.
//
// If the fetching of the data is not immediate, the method must listen for cancellation
// on the passed ctx. If the operation is stopped due to context cancellation, the
// function must return the result of the ctx.Err() method.
func (h *Handler) Find(ctx context.Context, lookup *resource.Lookup, page, perPage int) (*resource.ItemList, error) {

	p := make(map[string]interface{})
	l := make([]*resource.Item, 1)

	p["key1"] = "value1"
	p["key2"] = 2
	i, err := resource.NewItem(p)
	if err != nil {
		log.WithField("error", err).Warn("Error finding item(s)")
	}


	l = append(l, i)
	il := new(resource.ItemList)
	il.Page = 1
	il.Total = 1
	il.Items = l
	return il, err

}


// Insert stores new items in the backend store. If any of the items does already exist,
// no item should be inserted and a resource.ErrConflict must be returned. The insertion
// of the items must be performed atomically. If more than one item is provided and the
// backend store doesn't support atomical insertion of several items, a
// resource.ErrNotImplemented must be returned.
//
// If the storage of the data is not immediate, the method must listen for cancellation
// on the passed ctx. If the operation is stopped due to context cancellation, the
// function must return the result of the ctx.Err() method.
func (h *Handler) Insert (ctx context.Context, items []*resource.Item) error {

	tx_ptr, err := h.session.Begin()
	if err != nil {
		return err
	}
	for _, i := range items {
		// insert
		s := fmt.Sprintf("INSERT INTO %s(id, etag, updated, f1, f2) values(?,?,?,?,?)", h.tableName)
		stmt, err := h.session.Prepare(s)
		if err != nil {
			tx_ptr.Rollback()
			return err
		}

		_, err = stmt.Exec(i.ID, i.ETag, i.Updated, i.Payload["f1"], i.Payload["f2"])
		if err != nil {
			tx_ptr.Rollback()
			return err
		}
	}
	tx_ptr.Commit()
	return nil
}



// Update replace an item in the backend store by a new version. The ResourceHandler must
// ensure that the original item exists in the database and has the same Etag field.
// This check should be performed atomically. If the original item is not
// found, a resource.ErrNotFound must be returned. If the etags don't match, a
// resource.ErrConflict must be returned.
//
// The item payload must be stored together with the etag and the updated field.
// The item.ID and the payload["id"] is garantied to be identical, so there's not need
// to store both.
//
// If the storage of the data is not immediate, the method must listen for cancellation
// on the passed ctx. If the operation is stopped due to context cancellation, the
// function must return the result of the ctx.Err() method.
func (h *Handler) Update (ctx context.Context, item *resource.Item, original *resource.Item) error {

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
func (h *Handler) Delete (ctx context.Context, item *resource.Item) error {

	return resource.ErrNotFound
}


// Clear removes all items maching the lookup. When possible, the number of items
// removed is returned, otherwise -1 is return as the first value.
//
// The whole lookup query must be treated. If a query operation is not implemented
// by the storage handler, a resource.ErrNotImplemented must be returned.
//
// If the removal of the data is not immediate, the method must listen for cancellation
// on the passed ctx. If the operation is stopped due to context cancellation, the
// function must return the result of the ctx.Err() method.
func (h *Handler) Clear (ctx context.Context, lookup *resource.Lookup) (int, error) {

	return 0, resource.ErrNotImplemented

}
