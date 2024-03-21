package mysql

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	"sync/atomic"
)

type Rowsx struct {
	*sqlx.Rows
	closed atomic.Bool
}

func (r *Rowsx) Close() error {
	r.closed.Store(true)
	return r.Rows.Close()
}
func (r *Rowsx) IsClosed() bool {
	return r.closed.Load()
}

type Rows struct {
	*sql.Rows
	closed atomic.Bool
}

func (r *Rows) Close() error {
	r.closed.Store(true)
	return r.Rows.Close()
}
func (r *Rows) IsClosed() bool {
	return r.closed.Load()
}

type Row struct {
	*sql.Row
	err error
}

func (r *Row) Scan(args ...any) error {
	if r.err != nil {
		return r.err
	}
	return r.Row.Scan(args...)
}
func (r *Row) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.Row.Err()
}

type Rowx struct {
	*sqlx.Row
	err error
}

func (r *Rowx) Scan(args ...any) error {
	if r.err != nil {
		return r.err
	}
	return r.Row.Scan(args...)
}
func (r *Rowx) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.Row.Err()
}
func (r *Rowx) Columns() ([]string, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.Row.Columns()
}
func (r *Rowx) ColumnTypes() ([]*sql.ColumnType, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.Row.ColumnTypes()
}
func (r *Rowx) SliceScan() ([]interface{}, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.Row.SliceScan()
}
func (r *Rowx) MapScan(dest map[string]interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.Row.MapScan(dest)
}

func (r *Rowx) StructScan(dest interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.Row.StructScan(dest)
}
