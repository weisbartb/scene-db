package mysql

import (
	"context"
	"database/sql"
	"github.com/pkg/errors"
	"github.com/weisbartb/stack"
	"runtime"
	"strconv"

	// Needs to be imported for side-effects, without it this will fail to work properly
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// Defines a scannable entity that allows for a variety of results (rows vs row)
type Scannable interface {
	Scan(...any) error
}

// Core iterator for SQL rows
type Iter struct {
	rows *Rows
	err  error
}

func (i *Iter) For(scanner func(row Scannable) error) (err error) {
	if i.err != nil {
		return i.err
	}

	defer i.rows.Close()
	for i.rows.Next() {
		if err = scanner(i.rows); err != nil {
			return
		}
	}
	return
} // Defines a scannable entity that allows for a variety of results (rows vs row)

type rowCloser interface {
	Close() error
	IsClosed() bool
}

type Instance struct {
	ctx                context.Context
	db                 *sqlx.DB
	tx                 *sqlx.Tx
	children           []*Instance
	currentOpenRows    rowCloser
	lastOpenedLocation string
}

var ErrRowsNotClosed = errors.New("rows were not closed on active connection")
var ErrNoActiveTransaction = errors.New("no active transaction is present")
var ErrTransactionAlreadyStarted = errors.New("transaction has already started")

func NewInstance(ctx context.Context, db *sqlx.DB) *Instance {
	return &Instance{ctx: ctx, db: db}
}

func (d *Instance) SpawnChild() *Instance {
	i := &Instance{ctx: d.ctx, db: d.db}
	d.children = append(d.children, i)
	return i
}

// Isolate creates a new connection instance no longer attached to the context
func (d *Instance) Isolate() *Instance {
	i := &Instance{ctx: context.Background(), db: d.db}
	return i
}

// Raw gets the underlying SQL connection
func (d *Instance) Raw() *sqlx.DB {
	return d.db
}

// DriverName returns the name of the underpinned driver
func (d *Instance) DriverName() string {
	return d.db.DriverName()
}

// Rebind calls SQLx's rebind functionality
func (d *Instance) Rebind(query string) string {
	return d.db.Rebind(query)
}

// QueryFor runs a query and returns an iterable that can be accessed via
// err := db.QueryFor(...).For(func(row scannable){ ... })
// Errors from the underlying connection are automatically returned prior to the first invocation of the iterator func
func (d *Instance) QueryFor(query string, args ...any) *Iter {
	iter := &Iter{}
	iter.rows, iter.err = d.Query(query, args...)
	return iter
}

// Query runs a query and returns the sql rows and any applicable error
func (d *Instance) Query(query string, args ...any) (*Rows, error) {
	if err := d.unclosedCheck(); err != nil {
		return nil, err
	}
	var rows *sql.Rows
	var err error
	if d.tx != nil {
		rows, err = d.tx.QueryContext(d.ctx, query, args...)
	} else {
		rows, err = d.db.QueryContext(d.ctx, query, args...)
	}
	err = respErrorHandler(err)
	if err != nil {
		return nil, err
	}
	out := &Rows{
		Rows: rows,
	}
	return out, d.rowKeeper(out)
}

// Queryx runs a sqlx query command
func (d *Instance) Queryx(query string, args ...any) (*Rowsx, error) {
	if err := d.unclosedCheck(); err != nil {
		return nil, err
	}
	var rows *sqlx.Rows
	var err error
	if d.tx != nil {
		rows, err = d.tx.QueryxContext(d.ctx, query, args...)
	} else {
		rows, err = d.db.QueryxContext(d.ctx, query, args...)
	}
	err = respErrorHandler(err)
	if err != nil {
		return nil, err
	}
	out := &Rowsx{
		Rows: rows,
	}
	return out, d.rowKeeper(out)
}

// QueryRowx see sqlx.QueryRowx
func (d *Instance) QueryRowx(query string, args ...any) *Rowx {
	if d.currentOpenRows != nil && !d.currentOpenRows.IsClosed() {
		return &Rowx{err: ErrRowsNotClosed}
	}
	if d.tx != nil {
		return &Rowx{Row: d.tx.QueryRowxContext(d.ctx, query, args...)}
	}
	return &Rowx{Row: d.db.QueryRowxContext(d.ctx, query, args...)}
}

// QueryRow see sql.QueryRow
func (d *Instance) QueryRow(query string, args ...any) *Row {
	if d.currentOpenRows != nil && !d.currentOpenRows.IsClosed() {
		return &Row{err: ErrRowsNotClosed}
	}
	if d.tx != nil {
		return &Row{Row: d.tx.QueryRowContext(d.ctx, query, args...)}
	}
	return &Row{Row: d.db.QueryRowContext(d.ctx, query, args...)}
}

// Exec uses SQLx's Exec function
func (d *Instance) Exec(query string, args ...any) (sql.Result, error) {
	if err := d.unclosedCheck(); err != nil {
		return nil, err
	}
	var res sql.Result
	var err error
	if d.tx != nil {
		res, err = d.tx.ExecContext(d.ctx, query, args...)
	} else {
		res, err = d.db.ExecContext(d.ctx, query, args...)
	}
	err = respErrorHandler(err)
	return res, err
}

// BeginTx starts a transaction on this wrapper (and its connection)
// If a transaction is already present it will return an ErrTransactionAlreadyStarted
func (d *Instance) BeginTx(opts *sql.TxOptions) (err error) {
	if d.tx != nil {
		return ErrTransactionAlreadyStarted
	}
	d.tx, err = d.db.BeginTxx(d.ctx, opts)
	err = respErrorHandler(err)
	return
}

// Rollback will rill back any active transaction
// If no transaction is active, it will return ErrNoActiveTransaction which can be safely ignored
func (d *Instance) Rollback() error {
	if d.tx == nil {
		return ErrNoActiveTransaction
	}
	err := d.tx.Rollback()
	d.tx = nil
	return err
}

// Close will close out everything in the instance, open rows and open transactions.
// Any uncommitted transactions will be rolled back.
func (d *Instance) Close() []error {
	var err error
	var errors []error
	if d.currentOpenRows != nil {
		err = d.currentOpenRows.Close()
		if err != nil {
			errors = append(errors, err)
		}
	}
	if d.tx != nil {
		err = d.tx.Rollback()
		if err != nil {
			errors = append(errors, err)
		}
		d.tx = nil
	}
	for _, v := range d.children {
		errs := v.Close()
		if errs != nil {
			errors = append(errors, errs...)
		}
	}
	return errors
}

// Commit will commit the current transaction
// If no transaction is present it will return ErrNoActiveTransaction which can be safely ignored
func (d *Instance) Commit() error {
	if d.tx == nil {
		return ErrNoActiveTransaction
	}
	err := d.tx.Commit()
	if err == nil {
		d.tx = nil
	}
	return err
}

// InTx checks if a transaction is active
func (d *Instance) InTx() bool {
	return d.tx != nil
}

// PartialCommit performs a commit and then immediately opens a new transaction.
// NOTE: THIS DOES HOLD LOCKS FROM THE PREVIOUS TRANSACTION
func (d *Instance) PartialCommit() error {
	if d.tx == nil {
		return ErrNoActiveTransaction
	}
	_, err := d.Exec("COMMIT AND CHAIN NO RELEASE;")
	return err
}

// Ping will ping the db server
func (d *Instance) Ping() error {
	err := d.db.PingContext(d.ctx)
	return err
}

// RequireTx is used to have a critical section that requires beind under a transaction, if its already managed
// it will fall into the existing transaction. If there isn't a transaction active, it will autocommit if there is no error
func (d *Instance) RequireTx(f func(db *Instance) error) (err error) {
	if d.InTx() {
		return f(d)
	}
	err = d.BeginTx(nil)
	if err != nil {
		return stack.Trace(err)
	}
	defer d.Rollback()
	err = f(d)
	if err == nil {
		return d.Commit()
	}
	return err
}

func (d *Instance) unclosedCheck() error {
	if d.currentOpenRows != nil && !d.currentOpenRows.IsClosed() {
		return errors.Wrapf(ErrRowsNotClosed, "opened on %v", d.lastOpenedLocation)
	}
	return nil
}

func (d *Instance) rowKeeper(rows rowCloser) error {
	if d.currentOpenRows != nil {
		if err := d.currentOpenRows.Close(); err != nil {
			return errors.Wrapf(err, "opened on %v", d.lastOpenedLocation)
		}
	}
	_, file, line, _ := runtime.Caller(2)
	d.lastOpenedLocation = file + ":" + strconv.Itoa(line)
	d.currentOpenRows = rows
	return nil
}
