// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package gorp provides a simple way to marshal Go structs to and from
// SQL databases.  It uses the database/sql package, and should work with any
// compliant database/sql driver.
//
// Source code and project home:
// https://github.com/coopernurse/gorp
//
package gorp

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

const sliceIndexPlaceholder = -1

// TypeDeffer is any type that can be used as a column value in the database.
// This is mainly provided for table creation purposes.  You should use
// "database/sql".Scanner and "database/sql/driver".Valuer (or a TypeConverter)
// to implement data conversion.
//
// An example for a foreign key:
//
//     type User struct {
//         // Some user fields
//     }
//
//     func (u *User) TypeDef() string {
//         return "integer references users(user_id)"
//     }
//
// You probably don't want to actually hard code "users(user_id)", so don't
// copy that part of the example.  Take a look at DbMap.TableFor as an
// alternative.
type TypeDeffer interface {
	// TypeDef should return the string to use as the type's definition
	// (when used as a column) in the database.
	TypeDef() string
}

// Oracle String (empty string is null)
type OracleString struct {
	sql.NullString
}

// Scan implements the Scanner interface.
func (os *OracleString) Scan(value interface{}) error {
	if value == nil {
		os.String, os.Valid = "", false
		return nil
	}
	os.Valid = true
	return os.NullString.Scan(value)
}

// Value implements the driver Valuer interface.
func (os OracleString) Value() (driver.Value, error) {
	if !os.Valid || os.String == "" {
		return nil, nil
	}
	return os.String, nil
}

// A nullable Time value
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the Scanner interface.
func (nt *NullTime) Scan(value interface{}) error {
	nt.Time, nt.Valid = value.(time.Time)
	return nil
}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

var zeroVal reflect.Value
var versFieldConst = "[gorp_ver_field]"

// OptimisticLockError is returned by Update() or Delete() if the
// struct being modified has a Version field and the value is not equal to
// the current value in the database
type OptimisticLockError struct {
	// Table name where the lock error occurred
	TableName string

	// Primary key values of the row being updated/deleted
	Keys []interface{}

	// true if a row was found with those keys, indicating the
	// LocalVersion is stale.  false if no value was found with those
	// keys, suggesting the row has been deleted since loaded, or
	// was never inserted to begin with
	RowExists bool

	// Version value on the struct passed to Update/Delete. This value is
	// out of sync with the database.
	LocalVersion int64
}

// Error returns a description of the cause of the lock error
func (e OptimisticLockError) Error() string {
	if e.RowExists {
		return fmt.Sprintf("gorp: OptimisticLockError table=%s keys=%v out of date version=%d", e.TableName, e.Keys, e.LocalVersion)
	}

	return fmt.Sprintf("gorp: OptimisticLockError no row found for table=%s keys=%v", e.TableName, e.Keys)
}

// The TypeConverter interface provides a way to map a value of one
// type to another type when persisting to, or loading from, a database.
//
// Example use cases: Implement type converter to convert bool types to "y"/"n" strings,
// or serialize a struct member as a JSON blob.
type TypeConverter interface {
	// ToDb converts val to another type. Called before INSERT/UPDATE operations
	ToDb(val interface{}) (interface{}, error)

	// FromDb returns a CustomScanner appropriate for this type. This will be used
	// to hold values returned from SELECT queries.
	//
	// In particular the CustomScanner returned should implement a Binder
	// function appropriate for the Go type you wish to convert the db value to
	//
	// If bool==false, then no custom scanner will be used for this field.
	FromDb(target interface{}) (CustomScanner, bool)
}

// CustomScanner binds a database column value to a Go type
type CustomScanner struct {
	// After a row is scanned, Holder will contain the value from the database column.
	// Initialize the CustomScanner with the concrete Go type you wish the database
	// driver to scan the raw column into.
	Holder interface{}
	// Target typically holds a pointer to the target struct field to bind the Holder
	// value to.
	Target interface{}
	// Binder is a custom function that converts the holder value to the target type
	// and sets target accordingly.  This function should return error if a problem
	// occurs converting the holder to the target.
	Binder func(holder interface{}, target interface{}) error
}

// Bind is called automatically by gorp after Scan()
func (me CustomScanner) Bind() error {
	return me.Binder(me.Holder, me.Target)
}

// DbMap is the root gorp mapping object. Create one of these for each
// database schema you wish to map.  Each DbMap contains a list of
// mapped tables.
//
// Example:
//
//     dialect := gorp.MySQLDialect{"InnoDB", "UTF8"}
//     dbmap := &gorp.DbMap{Db: db, Dialect: dialect}
//
type DbMap struct {
	// Db handle to use with this map
	Db *sql.DB

	// Dialect implementation to use with this map
	Dialect Dialect

	TypeConverter TypeConverter

	tables    []*TableMap
	logger    GorpLogger
	logPrefix string
}

// TableMap represents a mapping between a Go struct and a database table
// Use dbmap.AddTable() or dbmap.AddTableWithName() to create these
type TableMap struct {
	// Name of database table.
	TableName      string
	SchemaName     string
	gotype         reflect.Type
	refval         reflect.Value
	Columns        []*ColumnMap
	keys           []*ColumnMap
	fkeys          []*ColumnMap
	uniqueTogether [][]string
	version        *ColumnMap
	insertPlan     bindPlan
	updatePlan     bindPlan
	deletePlan     bindPlan
	getPlan        bindPlan
	dbmap          *DbMap
}

func (t *TableMap) fkeyColumns(col *ColumnMap) []*ColumnMap {
	cols := make([]*ColumnMap, 0, len(col.targetTable.keys))
	for _, key := range col.targetTable.keys {
		cm := &ColumnMap{
			ColumnName: col.joinAlias + key.ColumnName,
			fieldIndex: append(col.fieldIndex, key.fieldIndex...),
			fieldName:  col.fieldName + "." + key.fieldName,
			origtype:   key.origtype,
			gotype:     key.gotype,
			joinAlias:  key.joinAlias,
			references: &Reference{
				table:  col.targetTable,
				column: key,
			},
		}
		key.referencedBy = append(key.referencedBy, &Reference{
			table:  t,
			column: cm,
		})
		cols = append(cols, cm)
	}
	// Check the target table for transient references to
	// this table.
	for _, targetCol := range col.targetTable.Columns {
		if targetCol.Transient && targetCol.isReference {
			targetType := targetCol.gotype
			for targetType.Kind() == reflect.Ptr || targetType.Kind() == reflect.Array || targetType.Kind() == reflect.Slice {
				targetType = targetType.Elem()
			}
			if t.gotype == targetType {
				targetCol.targetTable = t
			}
		}
	}
	return cols
}

func (t *TableMap) readStructColumns(v reflect.Value, typ reflect.Type) (cols []*ColumnMap, version *ColumnMap) {
	n := typ.NumField()
	for i := 0; i < n; i++ {
		f := typ.Field(i)
		var (
			columnName string
			options    []string
		)
		fakeAnonymous := false
		fkey := false
		joinAlias := ""
		// Find the name and options now, in case the field needs to *act*
		// as if it's embedded.
		columnName, options = t.dbmap.columnNameAndOptions(f)
		for _, option := range options {
			if option == "embed" {
				fakeAnonymous = true
			}
			if option == "fkey" {
				fkey = true
			}
			if strings.HasPrefix(option, "join") {
				splitIdx := strings.IndexRune(option, '=')
				if splitIdx > 0 {
					joinAlias = option[splitIdx+1:]
				}
			}
		}
		if fakeAnonymous && fkey {
			panic("Fields cannot be embedded into the parent table and used as foreign keys at the same time.")
		}
		var fv reflect.Value
		if v.Kind() != reflect.Invalid {
			fv = v.Field(i)
		}
		if (f.Anonymous || fakeAnonymous) && f.Type.Kind() == reflect.Struct && !fkey {
			// Recursively add nested fields in embedded structs.
			subcols, subversion := t.readStructColumns(fv, f.Type)
			// Don't append nested fields that have the same field
			// name as an already-mapped field.
			for _, subcol := range subcols {
				subcol.fieldIndex = append(f.Index, subcol.fieldIndex...)
				if fakeAnonymous {
					subcol.fieldName = fmt.Sprintf("%s.%s", f.Name, subcol.fieldName)
				}
				shouldAppend := true
				for _, col := range cols {
					if shouldAppend = subcol.fieldName != col.fieldName || subcol.Transient; !shouldAppend {
						break
					}
				}
				if shouldAppend {
					cols = append(cols, subcol)
				}
			}
			if subversion != nil {
				version = subversion
			}
			continue
		}
		origtype := f.Type
		gotype := origtype
		if t.dbmap.TypeConverter != nil {
			// Make a new pointer to a value of type gotype and
			// pass it to the TypeConverter's FromDb method to see
			// if a different type should be used for the column
			// type during table creation.
			value := reflect.New(gotype).Interface()
			scanner, useHolder := t.dbmap.TypeConverter.FromDb(value)
			if useHolder {
				gotype = reflect.TypeOf(scanner.Holder)
			}
		}
		var fieldRef interface{}
		if fv.IsValid() && t.refval.IsValid() {
			fieldRef = fv.Addr().Interface()
		}
		var targetTable *TableMap
		// transient foreign keys are intended to be available for
		// populating during a joined query, but should not be mapped
		// to a column.
		if fkey && columnName != "-" {
			targetType := f.Type
			if targetType.Kind() == reflect.Ptr {
				targetType = targetType.Elem()
			}
			if targetType.Kind() != reflect.Struct {
				panic("Foreign keys that are not structs or pointers to structs cannot be mapped to real columns (hint: `db:\"-,fkey\"`)")
			}
			if targetType == t.gotype {
				targetTable = t
			} else {
				var err error
				targetTable, err = t.dbmap.TableFor(targetType, true)
				if err != nil {
					panic(fmt.Errorf("Could not find previously mapped table for foreign key type %s: %s", targetType.Name(), err))
				}
			}
			// We want to include the parent field in the list of
			// columns, but not map it to an actual database column.
			columnName = "-"
		}
		cm := &ColumnMap{
			ColumnName:  columnName,
			Transient:   columnName == "-",
			fieldName:   f.Name,
			fieldIndex:  f.Index,
			fieldRef:    fieldRef,
			joinAlias:   joinAlias,
			targetTable: targetTable,
			origtype:    origtype,
			gotype:      gotype,
			isReference: fkey,
		}
		if targetTable != nil && targetTable != t {
			cols = append(cols, t.fkeyColumns(cm)...)
		}
		// Check for nested fields of the same field name and
		// override them.
		shouldAppend := true
		for index, col := range cols {
			if !col.Transient && col.fieldName == cm.fieldName {
				cols[index] = cm
				shouldAppend = false
				break
			}
		}
		if shouldAppend {
			cols = append(cols, cm)
		}
		if cm.fieldName == "Version" {
			version = cm
		}
	}
	return
}

// ManyToMany uses a field on t's type as a many-to-many mapping table
// between t and another table.  For example:
//
//     type Icon struct {
//         ID int
//     }
//
//     type UserIcon struct {
//         IconMapID `db:"id"`
//         Icon `db:",fkey"` // Alternatively, `Icon *Icon` `db:",fkey"`
//         User *User `db:",fkey"`
//         SortRank int `db:"sort_rank"`
//         Primary bool
//     }
//
//     type User struct {
//         ID int
//         Icons []*UserIcon `db:",fkey"`
//     }
//
//     dbMap.AddTable(Icon{}).SetKeys(true, "ID")
//     dbMap.AddTable(User{}).SetKeys(true, "ID").
//         ManyToMany("Icons").SetKeys(true, "IconMapID")
//
// It's important to note that UserIcon was never mapped to the
// database directly.  Really, this method doesn't do much more than
// calling AddTable and then ensuring that the table mapping has
// foreign keys pointing to t.
func (t *TableMap) ManyToMany(field interface{}) *TableMap {
	return t.ManyToManyWithNameAndSchema(field, "", "")
}

// ManyToManyWithName performs the same actions as ManyToMany, but
// with a table name for the mapping table.
func (t *TableMap) ManyToManyWithName(field interface{}, tablename string) *TableMap {
	return t.ManyToManyWithNameAndSchema(field, "", tablename)
}

// ManyToManyWithNameAndSchema performs the same actions as
// ManyToMany, but with a table and schema name for the mapping table.
func (t *TableMap) ManyToManyWithNameAndSchema(mapperType interface{}, schemaname, tablename string) *TableMap {
	targetType := reflect.TypeOf(mapperType)
	if targetType.Kind() == reflect.Ptr {
		targetType = targetType.Elem()
	}
	targetTable, err := t.dbmap.TableFor(targetType, true)
	if err == nil {
		panic("Cannot use already mapped type as mapping table")
	}
	// Try to use targetType as the mapping table.
	targetTable = t.dbmap.AddTableWithNameAndSchema(mapperType, schemaname, tablename)
	keysFound := false
	for _, fkey := range targetTable.fkeys {
		if fkey.references.table == t {
			keysFound = true
			break
		}
	}
	if !keysFound {
		msg := "Could not find a field in type %s referencing type %s"
		err = fmt.Errorf(msg, targetType.Name(), t.gotype.Name())
		panic(err)
	}
	return targetTable
}

// ResetSql removes cached insert/update/select/delete SQL strings
// associated with this TableMap.  Call this if you've modified
// any column names or the table name itself.
func (t *TableMap) ResetSql() {
	t.insertPlan = bindPlan{}
	t.updatePlan = bindPlan{}
	t.deletePlan = bindPlan{}
	t.getPlan = bindPlan{}
}

// SetKeys lets you specify the fields on a struct that map to primary
// key columns on the table.  If isAutoIncr is set, result.LastInsertId()
// will be used after INSERT to bind the generated id to the Go struct.
//
// Automatically calls ResetSql() to ensure SQL statements are regenerated.
//
// Panics if isAutoIncr is true, and fieldNames length != 1
//
func (t *TableMap) SetKeys(isAutoIncr bool, fields ...interface{}) *TableMap {
	if isAutoIncr && len(fields) != 1 {
		panic(fmt.Sprintf(
			"gorp: SetKeys: fields length must be 1 if key is auto-increment. (Saw %v fieldNames)",
			len(fields)))
	}
	if len(t.keys) > 0 {
		panic("gorp: SetKeys: cannot call SetKeys more than once")
	}
	t.keys = make([]*ColumnMap, 0, len(fields))
	for _, field := range fields {
		colmap := t.ColMap(field)
		colmap.isPK = true
		colmap.isAutoIncr = isAutoIncr
		t.keys = append(t.keys, colmap)
	}
	// Update self-referential foreign keys
	for _, col := range t.Columns {
		if col.targetTable == t {
			t.Columns = append(t.Columns, t.fkeyColumns(col)...)
		}
	}
	t.ResetSql()

	return t
}

// SetUniqueTogether lets you specify uniqueness constraints across multiple
// columns on the table. Each call adds an additional constraint for the
// specified columns.
//
// Automatically calls ResetSql() to ensure SQL statements are regenerated.
//
// Panics if fieldNames length < 2.
//
func (t *TableMap) SetUniqueTogether(fieldNames ...string) *TableMap {
	if len(fieldNames) < 2 {
		panic(fmt.Sprintf(
			"gorp: SetUniqueTogether: must provide at least two fieldNames to set uniqueness constraint."))
	}

	columns := make([]string, 0)
	for _, name := range fieldNames {
		columns = append(columns, name)
	}
	t.uniqueTogether = append(t.uniqueTogether, columns)
	t.ResetSql()

	return t
}

// ColMap returns the ColumnMap pointer matching the given struct field
// name.  It panics if the struct does not contain a field matching this
// name.
func (t *TableMap) ColMap(field interface{}) *ColumnMap {
	_, col := colMapOrNil(t, field)
	if col == nil {
		e := fmt.Sprintf("No ColumnMap in table %s type %s with field %v",
			t.TableName, t.gotype.Name(), field)

		panic(e)
	}
	return col
}

func colMapOrNil(t *TableMap, field interface{}) (multiJoinColumns []*ColumnMap, column *ColumnMap) {
	fieldName, isStr := field.(string)
	for _, col := range t.Columns {
		if isStr {
			if col.targetTable != nil && strings.HasPrefix(fieldName, col.JoinAlias()) {
				// This field may belong to a field within col's type.
				subMultiJoins, subcol := colMapOrNil(col.targetTable, fieldName[len(col.JoinAlias()):])
				if subcol != nil {
					multiJoinColumns = subMultiJoins
					// We need to return a copy of subcol, but with
					// its fieldIndex updated to include col's
					// fieldIndex
					subcolCopy := new(ColumnMap)
					*subcolCopy = *subcol
					index := col.fieldIndex
					if col.gotype.Kind() == reflect.Slice || col.gotype.Kind() == reflect.Array {
						index = append(index, sliceIndexPlaceholder)
						multiJoinColumns = append([]*ColumnMap{col}, multiJoinColumns...)
					}
					subcolCopy.fieldIndex = append(index, subcolCopy.fieldIndex...)
					column = subcolCopy
					return
				}
			}
			if col.fieldName == fieldName || col.ColumnName == fieldName || col.JoinAlias() == fieldName {
				return nil, col
			}
		} else if col.fieldRef == field {
			return nil, col
		}
	}
	return nil, nil
}

// SetVersionCol sets the column to use as the Version field.  By default
// the "Version" field is used.  Returns the column found, or panics
// if the struct does not contain a field matching this name.
//
// Automatically calls ResetSql() to ensure SQL statements are regenerated.
func (t *TableMap) SetVersionCol(field string) *ColumnMap {
	c := t.ColMap(field)
	t.version = c
	t.ResetSql()
	return c
}

func dbValue(value interface{}, conv TypeConverter) (interface{}, error) {
	if conv != nil {
		var err error
		value, err = conv.ToDb(value)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}

type bindPlan struct {
	query            string
	argFields        [][]int
	keyFields        [][]int
	versField        []int
	autoIncrIdx      int
	autoIncrFieldIdx []int
}

func (plan bindPlan) createBindInstance(elem reflect.Value, conv TypeConverter) (bindInstance, error) {
	bi := bindInstance{query: plan.query, autoIncrIdx: plan.autoIncrIdx, autoIncrFieldIdx: plan.autoIncrFieldIdx, versField: plan.versField}
	if plan.versField != nil {
		bi.existingVersion = elem.FieldByIndex(plan.versField).Int()
	}

	var err error

	for _, k := range plan.argFields {
		isUpdatedVersField := k == nil
		if isUpdatedVersField {
			k = plan.versField
		}
		field, _ := fieldByIndex(elem, k, false)
		if isUpdatedVersField {
			newVer := bi.existingVersion + 1
			bi.args = append(bi.args, newVer)
			if bi.existingVersion == 0 {
				field.SetInt(int64(newVer))
			}
			continue
		}
		val, err := dbValue(field.Interface(), conv)
		if err != nil {
			return bindInstance{}, err
		}
		bi.args = append(bi.args, val)
	}

	for _, k := range plan.keyFields {
		field, _ := fieldByIndex(elem, k, false)
		val := field.Interface()
		if conv != nil {
			val, err = conv.ToDb(val)
			if err != nil {
				return bindInstance{}, err
			}
		}
		bi.keys = append(bi.keys, val)
	}

	return bi, nil
}

type bindInstance struct {
	query            string
	args             []interface{}
	keys             []interface{}
	existingVersion  int64
	versField        []int
	autoIncrIdx      int
	autoIncrFieldIdx []int
}

func (t *TableMap) bindInsert(elem reflect.Value) (bindInstance, error) {
	plan := t.insertPlan
	if plan.query == "" {
		plan.autoIncrIdx = -1

		s := bytes.Buffer{}
		s2 := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("insert into %s (", t.dbmap.Dialect.QuotedTableForQuery(t.SchemaName, t.TableName)))

		x := 0
		first := true
		for y := range t.Columns {
			col := t.Columns[y]
			if !(col.isAutoIncr && t.dbmap.Dialect.AutoIncrBindValue() == "") {
				if !col.Transient {
					if !first {
						s.WriteString(",")
						s2.WriteString(",")
					}
					s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))

					if col.isAutoIncr {
						s2.WriteString(t.dbmap.Dialect.AutoIncrBindValue())
						plan.autoIncrIdx = y
						plan.autoIncrFieldIdx = col.fieldIndex
					} else {
						s2.WriteString(t.dbmap.Dialect.BindVar(x))
						if col == t.version {
							plan.versField = col.fieldIndex
							plan.argFields = append(plan.argFields, nil)
						} else {
							plan.argFields = append(plan.argFields, col.fieldIndex)
						}
						x++
					}
					first = false
				}
			} else {
				plan.autoIncrIdx = y
				plan.autoIncrFieldIdx = col.fieldIndex
			}
		}
		s.WriteString(") values (")
		s.WriteString(s2.String())
		s.WriteString(")")
		if plan.autoIncrIdx > -1 {
			s.WriteString(t.dbmap.Dialect.AutoIncrInsertSuffix(t.Columns[plan.autoIncrIdx]))
		}
		s.WriteString(t.dbmap.Dialect.QuerySuffix())

		plan.query = s.String()
		t.insertPlan = plan
	}

	return plan.createBindInstance(elem, t.dbmap.TypeConverter)
}

func (t *TableMap) bindUpdate(elem reflect.Value) (bindInstance, error) {
	plan := t.updatePlan
	if plan.query == "" {

		s := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("update %s set ", t.dbmap.Dialect.QuotedTableForQuery(t.SchemaName, t.TableName)))
		x := 0

		for y := range t.Columns {
			col := t.Columns[y]
			if !col.isAutoIncr && !col.Transient {
				if x > 0 {
					s.WriteString(", ")
				}
				s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))
				s.WriteString("=")
				s.WriteString(t.dbmap.Dialect.BindVar(x))

				if col == t.version {
					plan.versField = col.fieldIndex
					plan.argFields = append(plan.argFields, nil)
				} else {
					plan.argFields = append(plan.argFields, col.fieldIndex)
				}
				x++
			}
		}

		s.WriteString(" where ")
		for y := range t.keys {
			col := t.keys[y]
			if y > 0 {
				s.WriteString(" and ")
			}
			s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(x))

			plan.argFields = append(plan.argFields, col.fieldIndex)
			plan.keyFields = append(plan.keyFields, col.fieldIndex)
			x++
		}
		if plan.versField != nil {
			s.WriteString(" and ")
			s.WriteString(t.dbmap.Dialect.QuoteField(t.version.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(x))
			plan.argFields = append(plan.argFields, plan.versField)
		}
		s.WriteString(t.dbmap.Dialect.QuerySuffix())

		plan.query = s.String()
		t.updatePlan = plan
	}

	return plan.createBindInstance(elem, t.dbmap.TypeConverter)
}

func (t *TableMap) bindDelete(elem reflect.Value) (bindInstance, error) {
	plan := t.deletePlan
	if plan.query == "" {

		s := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("delete from %s", t.dbmap.Dialect.QuotedTableForQuery(t.SchemaName, t.TableName)))

		for y := range t.Columns {
			col := t.Columns[y]
			if !col.Transient {
				if col == t.version {
					plan.versField = col.fieldIndex
				}
			}
		}

		s.WriteString(" where ")
		for x := range t.keys {
			k := t.keys[x]
			if x > 0 {
				s.WriteString(" and ")
			}
			s.WriteString(t.dbmap.Dialect.QuoteField(k.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(x))

			plan.keyFields = append(plan.keyFields, k.fieldIndex)
			plan.argFields = append(plan.argFields, k.fieldIndex)
		}
		if plan.versField != nil {
			s.WriteString(" and ")
			s.WriteString(t.dbmap.Dialect.QuoteField(t.version.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(len(plan.argFields)))

			plan.argFields = append(plan.argFields, plan.versField)
		}
		s.WriteString(t.dbmap.Dialect.QuerySuffix())

		plan.query = s.String()
		t.deletePlan = plan
	}

	return plan.createBindInstance(elem, t.dbmap.TypeConverter)
}

func (t *TableMap) bindGet() bindPlan {
	plan := t.getPlan
	if plan.query == "" {

		s := bytes.Buffer{}
		s.WriteString("select ")

		x := 0
		for _, col := range t.Columns {
			if !col.Transient {
				if x > 0 {
					s.WriteString(",")
				}
				s.WriteString(t.dbmap.Dialect.QuotedTableForQuery(t.SchemaName, t.TableName))
				s.WriteString(".")
				s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))
				plan.argFields = append(plan.argFields, col.fieldIndex)
				x++
			}
		}
		s.WriteString(" from ")
		s.WriteString(t.dbmap.Dialect.QuotedTableForQuery(t.SchemaName, t.TableName))
		s.WriteString(" where ")
		for x := range t.keys {
			col := t.keys[x]
			if x > 0 {
				s.WriteString(" and ")
			}
			s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(x))

			plan.keyFields = append(plan.keyFields, col.fieldIndex)
		}
		s.WriteString(t.dbmap.Dialect.QuerySuffix())

		plan.query = s.String()
		t.getPlan = plan
	}

	return plan
}

// A Reference represents a foreign key relationship.
type Reference struct {
	table          *TableMap
	column         *ColumnMap
	cascadeDeletes bool
	cascadeUpdates bool
}

// OnDeleteCascade returns whether or not this reference will cascade
// on deletes.  Only used during table creation.
func (r *Reference) OnDeleteCascade() bool {
	return r.cascadeDeletes
}

// SetOnDeleteCascade sets whether or not this reference will be
// created with an `on delete cascade` clause.
func (r *Reference) SetOnDeleteCascade(cascade bool) {
	r.cascadeDeletes = cascade
}

// OnUpdateCascade returns whether or not this reference will cascade
// on updates.  Only used during table creation.
func (r *Reference) OnUpdateCascade() bool {
	return r.cascadeUpdates
}

// SetOnUpdateCascade sets whether or not this reference will be
// created with an `on update cascade` clause.
func (r *Reference) SetOnUpdateCascade(cascade bool) {
	r.cascadeUpdates = cascade
}

// Table returns the table that this reference points to.
func (r *Reference) Table() *TableMap {
	return r.table
}

// Column returns the column that this reference points to.
func (r *Reference) Column() *ColumnMap {
	return r.column
}

// ColumnMap represents a mapping between a Go struct field and a single
// column in a table.
// Unique and MaxSize only inform the
// CreateTables() function and are not used by Insert/Update/Delete/Get.
type ColumnMap struct {
	// Column name in db table
	ColumnName string

	// If true, this column is skipped in generated SQL statements
	Transient bool

	// If true, " unique" is added to create table statements.
	// Not used elsewhere
	Unique bool

	// Passed to Dialect.ToSqlType() to assist in informing the
	// correct column type to map to in CreateTables()
	// Not used elsewhere
	MaxSize int

	targetTable *TableMap

	fieldName  string
	fieldIndex []int
	fieldRef   interface{}

	// origtype is the type prior to calling TypeConverter methods
	origtype   reflect.Type
	gotype     reflect.Type
	isPK       bool
	isAutoIncr bool
	isNotNull  bool

	// isReference is used for transient references, to differentiate
	// them from normal transient fields.
	isReference  bool
	joinAlias    string
	references   *Reference
	referencedBy []*Reference
}

// TargetTable returns the *TableMap that this column represents, for
// foreign key fields.
func (c *ColumnMap) TargetTable() *TableMap {
	return c.targetTable
}

// FieldIndex returns the index (intended to be used by
// reflect.Value.FieldByIndex) for this column within its type.
func (c *ColumnMap) FieldIndex() []int {
	return c.fieldIndex
}

// JoinAlias returns a column name alias that can be used when
// performing joined queries.  Embedded structs that map to tables
// will prefix this to all child fields' join aliases.
func (c *ColumnMap) JoinAlias() string {
	return c.joinAlias
}

// SetJoinAlias sets the join alias for this column.
func (c *ColumnMap) SetJoinAlias(alias string) {
	c.joinAlias = alias
}

// References returns the table and column (in a *reference instance)
// which this column references as a foreign key.
//
// TODO: Add setter
func (c *ColumnMap) References() *Reference {
	return c.references
}

// ReferencedBy returns a list of all tables and columns (in a
// []*reference instance) which reference this column.  You cannot set
// this value directly.
func (c *ColumnMap) ReferencedBy() []*Reference {
	return c.referencedBy
}

// Rename allows you to specify the column name in the table
//
// Example:  table.ColMap("Updated").Rename("date_updated")
//
func (c *ColumnMap) Rename(colname string) *ColumnMap {
	c.ColumnName = colname
	return c
}

// SetTransient allows you to mark the column as transient. If true
// this column will be skipped when SQL statements are generated
func (c *ColumnMap) SetTransient(b bool) *ColumnMap {
	c.Transient = b
	return c
}

// SetUnique adds "unique" to the create table statements for this
// column, if b is true.
func (c *ColumnMap) SetUnique(b bool) *ColumnMap {
	c.Unique = b
	return c
}

// SetNotNull adds "not null" to the create table statements for this
// column, if nn is true.
func (c *ColumnMap) SetNotNull(nn bool) *ColumnMap {
	c.isNotNull = nn
	return c
}

// SetMaxSize specifies the max length of values of this column. This is
// passed to the dialect.ToSqlType() function, which can use the value
// to alter the generated type for "create table" statements
func (c *ColumnMap) SetMaxSize(size int) *ColumnMap {
	c.MaxSize = size
	return c
}

// Transaction represents a database transaction.
// Insert/Update/Delete/Get/Exec operations will be run in the context
// of that transaction.  Transactions should be terminated with
// a call to Commit() or Rollback()
type Transaction struct {
	dbmap  *DbMap
	tx     *sql.Tx
	closed bool
}

// SqlExecutor exposes gorp operations that can be run from Pre/Post
// hooks.  This hides whether the current operation that triggered the
// hook is in a transaction.
//
// See the DbMap function docs for each of the functions below for more
// information.
type SqlExecutor interface {
	Get(i interface{}, keys ...interface{}) (interface{}, error)
	Insert(list ...interface{}) error
	Update(list ...interface{}) (int64, error)
	Delete(list ...interface{}) (int64, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Select(i interface{}, query string,
		args ...interface{}) ([]interface{}, error)
	SelectInt(query string, args ...interface{}) (int64, error)
	SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error)
	SelectFloat(query string, args ...interface{}) (float64, error)
	SelectNullFloat(query string, args ...interface{}) (sql.NullFloat64, error)
	SelectStr(query string, args ...interface{}) (string, error)
	SelectNullStr(query string, args ...interface{}) (sql.NullString, error)
	SelectOne(holder interface{}, query string, args ...interface{}) error
	query(query string, args ...interface{}) (*sql.Rows, error)
	queryRow(query string, args ...interface{}) *sql.Row
}

// Compile-time check that DbMap and Transaction implement the SqlExecutor
// interface.
var _, _ SqlExecutor = &DbMap{}, &Transaction{}

type GorpLogger interface {
	Printf(format string, v ...interface{})
}

// TraceOn turns on SQL statement logging for this DbMap.  After this is
// called, all SQL statements will be sent to the logger.  If prefix is
// a non-empty string, it will be written to the front of all logged
// strings, which can aid in filtering log lines.
//
// Use TraceOn if you want to spy on the SQL statements that gorp
// generates.
//
// Note that the base log.Logger type satisfies GorpLogger, but adapters can
// easily be written for other logging packages (e.g., the golang-sanctioned
// glog framework).
func (m *DbMap) TraceOn(prefix string, logger GorpLogger) {
	m.logger = logger
	if prefix == "" {
		m.logPrefix = prefix
	} else {
		m.logPrefix = fmt.Sprintf("%s ", prefix)
	}
}

// TraceOff turns off tracing. It is idempotent.
func (m *DbMap) TraceOff() {
	m.logger = nil
	m.logPrefix = ""
}

// AddTable registers the given interface type with gorp. The table name
// will be given the name of the TypeOf(i).  You must call this function,
// or AddTableWithName, for any struct type you wish to persist with
// the given DbMap.
//
// This operation is idempotent. If i's type is already mapped, the
// existing *TableMap is returned
func (m *DbMap) AddTable(i interface{}) *TableMap {
	return m.AddTableWithName(i, "")
}

// AddTableWithName has the same behavior as AddTable, but sets
// table.TableName to name.
func (m *DbMap) AddTableWithName(i interface{}, name string) *TableMap {
	return m.AddTableWithNameAndSchema(i, "", name)
}

// AddTableWithNameAndSchema has the same behavior as AddTable, but sets
// table.TableName to name.
func (m *DbMap) AddTableWithNameAndSchema(i interface{}, schema string, name string) *TableMap {
	v := reflect.ValueOf(i)
	t := v.Type()
	var ref reflect.Value
	if t.Kind() == reflect.Ptr {
		if !v.IsNil() {
			ref = v
			v = v.Elem()
		}
		t = t.Elem()
	}
	if name == "" {
		name = t.Name()
	}

	// check if we have a table for this type already
	// if so, update the name and return the existing pointer
	for i := range m.tables {
		table := m.tables[i]
		if table.gotype == t {
			table.TableName = name
			return table
		}
	}

	tmap := &TableMap{gotype: t, refval: ref, TableName: name, SchemaName: schema, dbmap: m}
	tmap.Columns, tmap.version = tmap.readStructColumns(v, t)
	for _, col := range tmap.Columns {
		if col.references != nil {
			tmap.fkeys = append(tmap.fkeys, col)
		}
	}
	m.tables = append(m.tables, tmap)

	return tmap
}

func (m *DbMap) columnNameAndOptions(field reflect.StructField) (name string, options []string) {
	tagVars := strings.Split(field.Tag.Get("db"), ",")
	if len(tagVars) > 0 {
		name = tagVars[0]
	}
	if name == "" {
		name = strings.ToLower(field.Name)
	}
	if len(tagVars) > 1 {
		options = tagVars[1:]
	}
	return
}

// CreateTables iterates through TableMaps registered to this DbMap and
// executes "create table" statements against the database for each.
//
// This is particularly useful in unit tests where you want to create
// and destroy the schema automatically.
func (m *DbMap) CreateTables() error {
	return m.createTables(false)
}

// CreateTablesIfNotExists is similar to CreateTables, but starts
// each statement with "create table if not exists" so that existing
// tables do not raise errors
func (m *DbMap) CreateTablesIfNotExists() error {
	return m.createTables(true)
}

func (m *DbMap) createTables(ifNotExists bool) error {
	var err error
	for i := range m.tables {
		table := m.tables[i]

		s := bytes.Buffer{}

		if strings.TrimSpace(table.SchemaName) != "" {
			schemaCreate := "create schema"
			if ifNotExists {
				s.WriteString(m.Dialect.IfSchemaNotExists(schemaCreate, table.SchemaName))
			} else {
				s.WriteString(schemaCreate)
			}
			s.WriteString(fmt.Sprintf(" %s;", table.SchemaName))
		}

		tableCreate := "create table"
		if ifNotExists {
			s.WriteString(m.Dialect.IfTableNotExists(tableCreate, table.SchemaName, table.TableName))
		} else {
			s.WriteString(tableCreate)
		}
		s.WriteString(fmt.Sprintf(" %s (", m.Dialect.QuotedTableForQuery(table.SchemaName, table.TableName)))

		x := 0
		for _, col := range table.Columns {
			if !col.Transient {
				if x > 0 {
					s.WriteString(", ")
				}
				var stype string
				typer, ok := reflect.New(col.origtype).Interface().(TypeDeffer)
				if !ok && col.origtype != col.gotype {
					typer, ok = reflect.New(col.gotype).Interface().(TypeDeffer)
				}
				if ok {
					stype = typer.TypeDef()
				} else {
					stype = m.Dialect.ToSqlType(col.gotype, col.MaxSize, col.isAutoIncr)
				}
				s.WriteString(fmt.Sprintf("%s %s", m.Dialect.QuoteField(col.ColumnName), stype))

				if col.isPK || col.isNotNull {
					s.WriteString(" not null")
				}
				if col.isPK && len(table.keys) == 1 {
					s.WriteString(" primary key")
				}
				if col.Unique {
					s.WriteString(" unique")
				}
				ref := col.References()
				if ref != nil {
					s.WriteString(" references ")
					s.WriteString(m.Dialect.QuotedTableForQuery(ref.Table().SchemaName, ref.Table().TableName))
					s.WriteString("(")
					s.WriteString(m.Dialect.QuoteField(ref.Column().ColumnName))
					s.WriteString(")")
					if ref.OnDeleteCascade() {
						s.WriteString(" on delete cascade")
					}
					if ref.OnUpdateCascade() {
						s.WriteString(" on update cascade")
					}
				}
				if col.isAutoIncr {
					s.WriteString(fmt.Sprintf(" %s", m.Dialect.AutoIncrStr()))
				}

				x++
			}
		}
		if len(table.keys) > 1 {
			s.WriteString(", primary key (")
			for x := range table.keys {
				if x > 0 {
					s.WriteString(", ")
				}
				s.WriteString(m.Dialect.QuoteField(table.keys[x].ColumnName))
			}
			s.WriteString(")")
		}
		if len(table.uniqueTogether) > 0 {
			for _, columns := range table.uniqueTogether {
				s.WriteString(", unique (")
				for i, column := range columns {
					if i > 0 {
						s.WriteString(", ")
					}
					s.WriteString(m.Dialect.QuoteField(column))
				}
				s.WriteString(")")
			}
		}
		s.WriteString(") ")
		s.WriteString(m.Dialect.CreateTableSuffix())
		s.WriteString(m.Dialect.QuerySuffix())
		_, err = m.Exec(s.String())
		if err != nil {
			break
		}
	}
	return err
}

// DropTable drops an individual table.  Will throw an error
// if the table does not exist.
func (m *DbMap) DropTable(table interface{}) error {
	t := reflect.TypeOf(table)
	return m.dropTable(t, false)
}

// DropTable drops an individual table.  Will NOT throw an error
// if the table does not exist.
func (m *DbMap) DropTableIfExists(table interface{}) error {
	t := reflect.TypeOf(table)
	return m.dropTable(t, true)
}

// DropTables iterates through TableMaps registered to this DbMap and
// executes "drop table" statements against the database for each.
func (m *DbMap) DropTables() error {
	return m.dropTables(false)
}

// DropTablesIfExists is the same as DropTables, but uses the "if exists" clause to
// avoid errors for tables that do not exist.
func (m *DbMap) DropTablesIfExists() error {
	return m.dropTables(true)
}

// Goes through all the registered tables, dropping them one by one.
// If an error is encountered, then it is returned and the rest of
// the tables are not dropped.
func (m *DbMap) dropTables(addIfExists bool) (err error) {
	for _, table := range m.tables {
		err = m.dropTableImpl(table, addIfExists)
		if err != nil {
			return
		}
	}
	return err
}

// Implementation of dropping a single table.
func (m *DbMap) dropTable(t reflect.Type, addIfExists bool) error {
	table := tableOrNil(m, t)
	if table == nil {
		return errors.New(fmt.Sprintf("table %s was not registered!", table.TableName))
	}

	return m.dropTableImpl(table, addIfExists)
}

func (m *DbMap) dropTableImpl(table *TableMap, ifExists bool) (err error) {
	tableDrop := "drop table"
	if ifExists {
		tableDrop = m.Dialect.IfTableExists(tableDrop, table.SchemaName, table.TableName)
	}
	_, err = m.Exec(fmt.Sprintf("%s %s;", tableDrop, m.Dialect.QuotedTableForQuery(table.SchemaName, table.TableName)))
	return err
}

// TruncateTables iterates through TableMaps registered to this DbMap and
// executes "truncate table" statements against the database for each, or in the case of
// sqlite, a "delete from" with no "where" clause, which uses the truncate optimization
// (http://www.sqlite.org/lang_delete.html)
func (m *DbMap) TruncateTables() error {
	var err error
	for i := range m.tables {
		table := m.tables[i]
		_, e := m.Exec(fmt.Sprintf("%s %s;", m.Dialect.TruncateClause(), m.Dialect.QuotedTableForQuery(table.SchemaName, table.TableName)))
		if e != nil {
			err = e
		}
	}
	return err
}

// Insert runs a SQL INSERT statement for each element in list.  List
// items must be pointers.
//
// Any interface whose TableMap has an auto-increment primary key will
// have its last insert id bound to the PK field on the struct.
//
// The hook functions PreInsert() and/or PostInsert() will be executed
// before/after the INSERT statement if the interface defines them.
//
// Panics if any interface in the list has not been registered with AddTable
func (m *DbMap) Insert(list ...interface{}) error {
	return insert(m, m, list...)
}

// Update runs a SQL UPDATE statement for each element in list.  List
// items must be pointers.
//
// The hook functions PreUpdate() and/or PostUpdate() will be executed
// before/after the UPDATE statement if the interface defines them.
//
// Returns the number of rows updated.
//
// Returns an error if SetKeys has not been called on the TableMap
// Panics if any interface in the list has not been registered with AddTable
func (m *DbMap) Update(list ...interface{}) (int64, error) {
	return update(m, m, list...)
}

// Delete runs a SQL DELETE statement for each element in list.  List
// items must be pointers.
//
// The hook functions PreDelete() and/or PostDelete() will be executed
// before/after the DELETE statement if the interface defines them.
//
// Returns the number of rows deleted.
//
// Returns an error if SetKeys has not been called on the TableMap
// Panics if any interface in the list has not been registered with AddTable
func (m *DbMap) Delete(list ...interface{}) (int64, error) {
	return delete(m, m, list...)
}

// Get runs a SQL SELECT to fetch a single row from the table based on the
// primary key(s)
//
// i should be an empty value for the struct to load.  keys should be
// the primary key value(s) for the row to load.  If multiple keys
// exist on the table, the order should match the column order
// specified in SetKeys() when the table mapping was defined.
//
// The hook function PostGet() will be executed after the SELECT
// statement if the interface defines them.
//
// Returns a pointer to a struct that matches or nil if no row is found.
//
// Returns an error if SetKeys has not been called on the TableMap
// Panics if any interface in the list has not been registered with AddTable
func (m *DbMap) Get(i interface{}, keys ...interface{}) (interface{}, error) {
	return get(m, m, i, keys...)
}

// Select runs an arbitrary SQL query, binding the columns in the result
// to fields on the struct specified by i.  args represent the bind
// parameters for the SQL statement.
//
// Column names on the SELECT statement should be aliased to the field names
// on the struct i. Returns an error if one or more columns in the result
// do not match.  It is OK if fields on i are not part of the SQL
// statement.
//
// The hook function PostGet() will be executed after the SELECT
// statement if the interface defines them.
//
// Values are returned in one of two ways:
// 1. If i is a struct or a pointer to a struct, returns a slice of pointers to
// matching rows of type i.
// 2. If i is a pointer to a slice, the results will be appended to that slice
// and nil returned.
//
// i does NOT need to be registered with AddTable()
func (m *DbMap) Select(i interface{}, query string, args ...interface{}) ([]interface{}, error) {
	return hookedselect(m, m, i, query, args...)
}

// Exec runs an arbitrary SQL statement.  args represent the bind parameters.
// This is equivalent to running:  Exec() using database/sql
func (m *DbMap) Exec(query string, args ...interface{}) (sql.Result, error) {
	m.trace(query, args...)
	return m.Db.Exec(query, args...)
}

// SelectInt is a convenience wrapper around the gorp.SelectInt function
func (m *DbMap) SelectInt(query string, args ...interface{}) (int64, error) {
	return SelectInt(m, query, args...)
}

// SelectNullInt is a convenience wrapper around the gorp.SelectNullInt function
func (m *DbMap) SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error) {
	return SelectNullInt(m, query, args...)
}

// SelectFloat is a convenience wrapper around the gorp.SelectFlot function
func (m *DbMap) SelectFloat(query string, args ...interface{}) (float64, error) {
	return SelectFloat(m, query, args...)
}

// SelectNullFloat is a convenience wrapper around the gorp.SelectNullFloat function
func (m *DbMap) SelectNullFloat(query string, args ...interface{}) (sql.NullFloat64, error) {
	return SelectNullFloat(m, query, args...)
}

// SelectStr is a convenience wrapper around the gorp.SelectStr function
func (m *DbMap) SelectStr(query string, args ...interface{}) (string, error) {
	return SelectStr(m, query, args...)
}

// SelectNullStr is a convenience wrapper around the gorp.SelectNullStr function
func (m *DbMap) SelectNullStr(query string, args ...interface{}) (sql.NullString, error) {
	return SelectNullStr(m, query, args...)
}

// SelectOne is a convenience wrapper around the gorp.SelectOne function
func (m *DbMap) SelectOne(holder interface{}, query string, args ...interface{}) error {
	return SelectOne(m, m, holder, query, args...)
}

// Begin starts a gorp Transaction
func (m *DbMap) Begin() (*Transaction, error) {
	m.trace("begin;")
	tx, err := m.Db.Begin()
	if err != nil {
		return nil, err
	}
	return &Transaction{m, tx, false}, nil
}

// TableFor returns the *TableMap corresponding to the given Go Type
// If no table is mapped to that type an error is returned.
// If checkPK is true and the mapped table has no registered PKs, an error is returned.
func (m *DbMap) TableFor(t reflect.Type, checkPK bool) (*TableMap, error) {
	table := tableOrNil(m, t)
	if table == nil {
		return nil, errors.New(fmt.Sprintf("No table found for type: %v", t.Name()))
	}

	if checkPK && len(table.keys) < 1 {
		e := fmt.Sprintf("gorp: No keys defined for table: %s",
			table.TableName)
		return nil, errors.New(e)
	}

	return table, nil
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the returned statement.
// This is equivalent to running:  Prepare() using database/sql
func (m *DbMap) Prepare(query string) (*sql.Stmt, error) {
	m.trace(query, nil)
	return m.Db.Prepare(query)
}

func tableOrNil(m *DbMap, t reflect.Type) *TableMap {
	for i := range m.tables {
		table := m.tables[i]
		if table.gotype == t {
			return table
		}
	}
	return nil
}

func (m *DbMap) tableForPointer(ptr interface{}, checkPK bool) (*TableMap, reflect.Value, error) {
	ptrv := reflect.ValueOf(ptr)
	if ptrv.Kind() != reflect.Ptr {
		e := fmt.Sprintf("gorp: passed non-pointer: %v (kind=%v)", ptr,
			ptrv.Kind())
		return nil, reflect.Value{}, errors.New(e)
	}
	elem := ptrv.Elem()
	etype := reflect.TypeOf(elem.Interface())
	t, err := m.TableFor(etype, checkPK)
	if err != nil {
		return nil, reflect.Value{}, err
	}

	return t, elem, nil
}

func (m *DbMap) queryRow(query string, args ...interface{}) *sql.Row {
	m.trace(query, args...)
	return m.Db.QueryRow(query, args...)
}

func (m *DbMap) query(query string, args ...interface{}) (*sql.Rows, error) {
	if m.TypeConverter != nil {
		var convErr error
		for index, value := range args {
			value, convErr = dbValue(value, m.TypeConverter)
			if convErr != nil {
				return nil, convErr
			}
			args[index] = value
		}
	}
	m.trace(query, args...)
	return m.Db.Query(query, args...)
}

func (m *DbMap) trace(query string, args ...interface{}) {
	if m.logger != nil {
		var margs = argsString(args...)
		m.logger.Printf("%s%s [%s]", m.logPrefix, query, margs)
	}
}

func argsString(args ...interface{}) string {
	var margs string
	for i, a := range args {
		var v interface{} = a
		if x, ok := v.(driver.Valuer); ok {
			y, err := x.Value()
			if err == nil {
				v = y
			}
		}
		switch v.(type) {
		case string:
			v = fmt.Sprintf("%q", v)
		default:
			v = fmt.Sprintf("%v", v)
		}
		margs += fmt.Sprintf("%d:%s", i+1, v)
		if i+1 < len(args) {
			margs += " "
		}
	}
	return margs
}

///////////////

// Insert has the same behavior as DbMap.Insert(), but runs in a transaction.
func (t *Transaction) Insert(list ...interface{}) error {
	return insert(t.dbmap, t, list...)
}

// Update had the same behavior as DbMap.Update(), but runs in a transaction.
func (t *Transaction) Update(list ...interface{}) (int64, error) {
	return update(t.dbmap, t, list...)
}

// Delete has the same behavior as DbMap.Delete(), but runs in a transaction.
func (t *Transaction) Delete(list ...interface{}) (int64, error) {
	return delete(t.dbmap, t, list...)
}

// Get has the same behavior as DbMap.Get(), but runs in a transaction.
func (t *Transaction) Get(i interface{}, keys ...interface{}) (interface{}, error) {
	return get(t.dbmap, t, i, keys...)
}

// Select has the same behavior as DbMap.Select(), but runs in a transaction.
func (t *Transaction) Select(i interface{}, query string, args ...interface{}) ([]interface{}, error) {
	return hookedselect(t.dbmap, t, i, query, args...)
}

// Exec has the same behavior as DbMap.Exec(), but runs in a transaction.
func (t *Transaction) Exec(query string, args ...interface{}) (sql.Result, error) {
	t.dbmap.trace(query, args...)
	return t.tx.Exec(query, args...)
}

// SelectInt is a convenience wrapper around the gorp.SelectInt function.
func (t *Transaction) SelectInt(query string, args ...interface{}) (int64, error) {
	return SelectInt(t, query, args...)
}

// SelectNullInt is a convenience wrapper around the gorp.SelectNullInt function.
func (t *Transaction) SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error) {
	return SelectNullInt(t, query, args...)
}

// SelectFloat is a convenience wrapper around the gorp.SelectFloat function.
func (t *Transaction) SelectFloat(query string, args ...interface{}) (float64, error) {
	return SelectFloat(t, query, args...)
}

// SelectNullFloat is a convenience wrapper around the gorp.SelectNullFloat function.
func (t *Transaction) SelectNullFloat(query string, args ...interface{}) (sql.NullFloat64, error) {
	return SelectNullFloat(t, query, args...)
}

// SelectStr is a convenience wrapper around the gorp.SelectStr function.
func (t *Transaction) SelectStr(query string, args ...interface{}) (string, error) {
	return SelectStr(t, query, args...)
}

// SelectNullStr is a convenience wrapper around the gorp.SelectNullStr function.
func (t *Transaction) SelectNullStr(query string, args ...interface{}) (sql.NullString, error) {
	return SelectNullStr(t, query, args...)
}

// SelectOne is a convenience wrapper around the gorp.SelectOne function.
func (t *Transaction) SelectOne(holder interface{}, query string, args ...interface{}) error {
	return SelectOne(t.dbmap, t, holder, query, args...)
}

// Commit commits the underlying database transaction.
func (t *Transaction) Commit() error {
	if !t.closed {
		t.closed = true
		t.dbmap.trace("commit;")
		return t.tx.Commit()
	}

	return sql.ErrTxDone
}

// Rollback rolls back the underlying database transaction.
func (t *Transaction) Rollback() error {
	if !t.closed {
		t.closed = true
		t.dbmap.trace("rollback;")
		return t.tx.Rollback()
	}

	return sql.ErrTxDone
}

// Savepoint creates a savepoint with the given name. The name is interpolated
// directly into the SQL SAVEPOINT statement, so you must sanitize it if it is
// derived from user input.
func (t *Transaction) Savepoint(name string) error {
	query := "savepoint " + t.dbmap.Dialect.QuoteField(name)
	t.dbmap.trace(query, nil)
	_, err := t.tx.Exec(query)
	return err
}

// RollbackToSavepoint rolls back to the savepoint with the given name. The
// name is interpolated directly into the SQL SAVEPOINT statement, so you must
// sanitize it if it is derived from user input.
func (t *Transaction) RollbackToSavepoint(savepoint string) error {
	query := "rollback to savepoint " + t.dbmap.Dialect.QuoteField(savepoint)
	t.dbmap.trace(query, nil)
	_, err := t.tx.Exec(query)
	return err
}

// ReleaseSavepint releases the savepoint with the given name. The name is
// interpolated directly into the SQL SAVEPOINT statement, so you must sanitize
// it if it is derived from user input.
func (t *Transaction) ReleaseSavepoint(savepoint string) error {
	query := "release savepoint " + t.dbmap.Dialect.QuoteField(savepoint)
	t.dbmap.trace(query, nil)
	_, err := t.tx.Exec(query)
	return err
}

// Prepare has the same behavior as DbMap.Prepare(), but runs in a transaction.
func (t *Transaction) Prepare(query string) (*sql.Stmt, error) {
	t.dbmap.trace(query, nil)
	return t.tx.Prepare(query)
}

func (t *Transaction) queryRow(query string, args ...interface{}) *sql.Row {
	t.dbmap.trace(query, args...)
	return t.tx.QueryRow(query, args...)
}

func (t *Transaction) query(query string, args ...interface{}) (*sql.Rows, error) {
	t.dbmap.trace(query, args...)
	return t.tx.Query(query, args...)
}

///////////////

// SelectInt executes the given query, which should be a SELECT statement for a single
// integer column, and returns the value of the first row returned.  If no rows are
// found, zero is returned.
func SelectInt(e SqlExecutor, query string, args ...interface{}) (int64, error) {
	var h int64
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	return h, nil
}

// SelectNullInt executes the given query, which should be a SELECT statement for a single
// integer column, and returns the value of the first row returned.  If no rows are
// found, the empty sql.NullInt64 value is returned.
func SelectNullInt(e SqlExecutor, query string, args ...interface{}) (sql.NullInt64, error) {
	var h sql.NullInt64
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return h, err
	}
	return h, nil
}

// SelectFloat executes the given query, which should be a SELECT statement for a single
// float column, and returns the value of the first row returned. If no rows are
// found, zero is returned.
func SelectFloat(e SqlExecutor, query string, args ...interface{}) (float64, error) {
	var h float64
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	return h, nil
}

// SelectNullFloat executes the given query, which should be a SELECT statement for a single
// float column, and returns the value of the first row returned. If no rows are
// found, the empty sql.NullInt64 value is returned.
func SelectNullFloat(e SqlExecutor, query string, args ...interface{}) (sql.NullFloat64, error) {
	var h sql.NullFloat64
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return h, err
	}
	return h, nil
}

// SelectStr executes the given query, which should be a SELECT statement for a single
// char/varchar column, and returns the value of the first row returned.  If no rows are
// found, an empty string is returned.
func SelectStr(e SqlExecutor, query string, args ...interface{}) (string, error) {
	var h string
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}
	return h, nil
}

// SelectNullStr executes the given query, which should be a SELECT
// statement for a single char/varchar column, and returns the value
// of the first row returned.  If no rows are found, the empty
// sql.NullString is returned.
func SelectNullStr(e SqlExecutor, query string, args ...interface{}) (sql.NullString, error) {
	var h sql.NullString
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return h, err
	}
	return h, nil
}

// SelectOne executes the given query (which should be a SELECT statement)
// and binds the result to holder, which must be a pointer.
//
// If no row is found, an error (sql.ErrNoRows specifically) will be returned
//
// If more than one row is found, an error will be returned.
//
func SelectOne(m *DbMap, e SqlExecutor, holder interface{}, query string, args ...interface{}) error {
	t := reflect.TypeOf(holder)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	} else {
		return fmt.Errorf("gorp: SelectOne holder must be a pointer, but got: %t", holder)
	}

	// Handle pointer to pointer
	isptr := false
	if t.Kind() == reflect.Ptr {
		isptr = true
		t = t.Elem()
	}

	if t.Kind() == reflect.Struct {
		var nonFatalErr error

		list, err := hookedselect(m, e, holder, query, args...)
		if err != nil {
			if !NonFatalError(err) {
				return err
			}
			nonFatalErr = err
		}

		dest := reflect.ValueOf(holder)
		if isptr {
			dest = dest.Elem()
		}

		if list != nil && len(list) > 0 {
			// check for multiple rows
			if len(list) > 1 {
				return fmt.Errorf("gorp: multiple rows returned for: %s - %v", query, args)
			}

			// Initialize if nil
			if dest.IsNil() {
				dest.Set(reflect.New(t))
			}

			// only one row found
			src := reflect.ValueOf(list[0])
			dest.Elem().Set(src.Elem())
		} else {
			// No rows found, return a proper error.
			return sql.ErrNoRows
		}

		return nonFatalErr
	}

	return selectVal(e, holder, query, args...)
}

func selectVal(e SqlExecutor, holder interface{}, query string, args ...interface{}) error {
	if len(args) == 1 {
		switch m := e.(type) {
		case *DbMap:
			query, args = maybeExpandNamedQuery(m, query, args)
		case *Transaction:
			query, args = maybeExpandNamedQuery(m.dbmap, query, args)
		}
	}
	rows, err := e.query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return sql.ErrNoRows
	}

	return rows.Scan(holder)
}

///////////////

func hookedselect(m *DbMap, exec SqlExecutor, i interface{}, query string,
	args ...interface{}) ([]interface{}, error) {

	var nonFatalErr error

	list, err := rawselect(m, exec, i, query, args...)
	if err != nil {
		if !NonFatalError(err) {
			return nil, err
		}
		nonFatalErr = err
	}

	// Determine where the results are: written to i, or returned in list
	if t, _ := toSliceType(i); t == nil {
		for _, v := range list {
			if v, ok := v.(HasPostGet); ok {
				err := v.PostGet(exec)
				if err != nil {
					return nil, err
				}
			}
		}
	} else {
		resultsValue := reflect.Indirect(reflect.ValueOf(i))
		for i := 0; i < resultsValue.Len(); i++ {
			if v, ok := resultsValue.Index(i).Interface().(HasPostGet); ok {
				err := v.PostGet(exec)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return list, nonFatalErr
}

type fieldBinder struct {
	holder          reflect.Value
	parentNilSetter reflect.Value
	nilSetter       reflect.Value
	valueSetter     reflect.Value
	binder          binder

	retErr error
}

func (b *fieldBinder) Bind() error {
	val := b.holder
	valIsNil := false
	if b.nilSetter.IsValid() {
		// If the field can be set to nil, then b.holder is guaranteed
		// to be nillable.
		valIsNil = val.IsNil()
		if valIsNil && val != b.nilSetter {
			// since val (which is b.holder) is not equal to
			// b.nilSetter, b.binder.Bind() does not need to be
			// called.
			if !b.nilSetter.IsNil() {
				b.nilSetter.Set(reflect.Zero(b.nilSetter.Type()))
			}
			return nil
		}
		if !valIsNil {
			val = val.Elem()
		}
	}
	// if b.valueSetter is not valid, it means that the holder was the
	// field, so there's nothing more to do.
	if !valIsNil && b.valueSetter.IsValid() {
		b.valueSetter.Set(val)
	}
	if b.binder != nil {
		err := b.binder.Bind()
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *fieldBinder) setScanner(scanner CustomScanner) {
	b.binder = scanner
	newHolder := reflect.ValueOf(scanner.Holder)
	if newHolder.Kind() != reflect.Ptr {
		panic(fmt.Errorf("Cannot use non-pointer type %s as holder", newHolder.Type().Name()))
	}
	if b.holder == b.nilSetter {
		b.nilSetter = b.parentNilSetter
	}
	b.holder = newHolder.Elem()
	b.valueSetter = b.holder
	if b.holder.Kind() == reflect.Ptr {
		// The CustomScanner probably wants to handle nil values on
		// its own.
		b.valueSetter = b.valueSetter.Elem()
		b.nilSetter = b.holder
	} else if b.nilSetter.IsValid() {
		// We still want to allow nil values.
		b.holder = reflect.New(newHolder.Type()).Elem()
	}
}

// fieldByIndex is a copy of v.FieldByIndex, with the following
// changes:
//
// 1. If initialize is true, it will initialize pointers that it
// descends through, sidestepping the "indirection through nil pointer
// to embedded struct" error.
//
// 2. If initialize is true and it descends through a pointer to a
// struct and returns a non-pointer field, it will also return a
// custom scanner that will set the nearest struct pointer to nil if
// the database value is nil, instead of trying to scan a nil value to
// a non-nil field.
//
// 3. If initialize is false and it finds a nil pointer, it will
// return nil rather than panicking.
func fieldByIndex(v reflect.Value, index []int, initialize bool) (reflect.Value, *fieldBinder) {
	var (
		// A quick paragraph about these:
		//
		// If we get nil values in a select statement for joins, we
		// want to empty the nearest parent struct or pointer.
		// However, there's an exception: if the direct parent of a
		// pointer is a list, then we still want to empty the list
		// instead of emptying the pointer.
		//
		// So we need to store the most direct parent to empty; if
		// it's set, then we need to make a custom scanner that
		// empties it if a nil value is received.
		lastListDepth         = -1
		emptyingValue         reflect.Value
		previousEmptyingValue reflect.Value
	)
	for i, idx := range index {
		if v.Kind() == reflect.Ptr {
			// If the difference in depth is only 1, then this
			// pointer type is the list's element type, and we
			// still want to empty the list on nil values.
			// Anything more, though, and this is nested
			// deeper and we just want to set the pointer to
			// nil.
			//
			// Note that 0 - -1 = 1 and therefor the equality
			// will always be false at the top level.
			if i-lastListDepth > 1 {
				lastListDepth = -1
				previousEmptyingValue = emptyingValue
				emptyingValue = v
			}
			if v.IsNil() {
				if !initialize {
					return v, nil
				}
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		switch v.Kind() {
		case reflect.Struct:
			v = v.Field(idx)
		case reflect.Slice, reflect.Array:
			previousEmptyingValue = emptyingValue
			emptyingValue = v
			lastListDepth = i
			if idx == sliceIndexPlaceholder {
				// The placeholder means to return the first element,
				// creating a first element if necessary.
				idx = 0
				if v.Len() == 0 {
					newV := reflect.New(v.Type().Elem()).Elem()
					v.Set(reflect.Append(v, newV))
				}
			}
			v = v.Index(idx)
		default:
			panic("gorp: found unsupported type using fieldByIndex")
		}
	}

	// If initialize is true, then this is a select statement, so we
	// need to provide a fieldBinder
	var b *fieldBinder
	if initialize {
		b = &fieldBinder{
			parentNilSetter: previousEmptyingValue,
			nilSetter:       emptyingValue,
			holder:          v,
		}
		if emptyingValue.IsValid() && v != emptyingValue {
			if v.Kind() == reflect.Ptr {
				// This is unlikely, since most of the time v and
				// emptyingValue will be equal when v is a pointer,
				// but it can happen.
				if v.IsNil() {
					v.Set(reflect.New(v.Type().Elem()))
				}
				b.valueSetter = v.Elem()
			} else {
				// Make holder an addressable, nillable value.
				b.holder = reflect.New(v.Addr().Type()).Elem()
				b.valueSetter = v
			}
		}
	}
	return v, b
}

type binder interface {
	Bind() error
}

func rawselect(m *DbMap, exec SqlExecutor, i interface{}, query string,
	args ...interface{}) ([]interface{}, error) {
	var (
		appendToSlice   = false // Write results to i directly?
		intoStruct      = true  // Selecting into a struct?
		pointerElements = true  // Are the slice elements pointers (vs values)?
	)

	var nonFatalErr error

	// get type for i, verifying it's a supported destination
	t, err := toType(i)
	if err != nil {
		var err2 error
		if t, err2 = toSliceType(i); t == nil {
			if err2 != nil {
				return nil, err2
			}
			return nil, err
		}
		pointerElements = t.Kind() == reflect.Ptr
		if pointerElements {
			t = t.Elem()
		}
		appendToSlice = true
		intoStruct = t.Kind() == reflect.Struct
	}

	// If the caller supplied a single struct/map argument, assume a "named
	// parameter" query.  Extract the named arguments from the struct/map, create
	// the flat arg slice, and rewrite the query to use the dialect's placeholder.
	if len(args) == 1 {
		query, args = maybeExpandNamedQuery(m, query, args)
	}

	// Run the query
	rows, err := exec.query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Fetch the column names as returned from db
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if !intoStruct && len(cols) > 1 {
		return nil, fmt.Errorf("gorp: select into non-struct slice requires 1 column, got %d", len(cols))
	}

	var (
		colToFieldIndex [][]int
		multiJoinCols   [][]*ColumnMap
		table           *TableMap
	)
	if intoStruct {
		if colToFieldIndex, table, multiJoinCols, err = columnToFieldIndex(m, t, cols); err != nil {
			if !NonFatalError(err) {
				return nil, err
			}
			nonFatalErr = err
		}
	}

	conv := m.TypeConverter

	// Add results to one of these two slices.
	var (
		list       = make([]interface{}, 0)
		sliceValue = reflect.Indirect(reflect.ValueOf(i))
	)

	// We'll store types with keys that have already been read (for
	// one-to-many or many-to-many queries) in a map.
	parsedRows := make(map[reflect.Type]map[string]reflect.Value)
	for {
		if !rows.Next() {
			// if error occured return rawselect
			if rows.Err() != nil {
				return nil, rows.Err()
			}
			// time to exit from outer "for" loop
			break
		}
		v := reflect.New(t)
		dest := make([]interface{}, len(cols))

		custScan := make([]binder, 0)

		for x := range cols {
			f := v.Elem()
			var bind *fieldBinder
			if intoStruct {

				index := colToFieldIndex[x]
				if index == nil {
					// this field is not present in the struct, so create a dummy
					// value for rows.Scan to scan into
					var dummy sql.RawBytes
					dest[x] = &dummy
					continue
				}
				f, bind = fieldByIndex(f, index, true)
				if bind != nil {
					custScan = append(custScan, bind)
				}
			}
			target := f.Addr().Interface()
			if conv != nil {
				scanner, ok := conv.FromDb(target)
				if ok {
					if bind != nil {
						// The fieldBinder will call scanner.Bind().
						bind.setScanner(scanner)
					} else {
						target = scanner.Holder
						custScan = append(custScan, scanner)
					}
				}
			}
			if bind != nil {
				target = bind.holder.Addr().Interface()
			}
			dest[x] = target
		}

		err = rows.Scan(dest...)
		if err != nil {
			return nil, err
		}

		for _, c := range custScan {
			err = c.Bind()
			if err != nil {
				return nil, err
			}
		}

		shouldAppend := true
		if multiJoinCols != nil && table != nil {
			// There are slice elements in the type that are being
			// filled out by values in the query.  This means the
			// select is probably a many-to-many or one-to-many join
			// statement, and we need to deal with the possibility
			// that parts of this row have already been loaded.

			found := handleMultiJoin(v.Elem(), table, multiJoinCols, parsedRows)
			shouldAppend = !found
		}

		if shouldAppend {
			if appendToSlice {
				if !pointerElements {
					v = v.Elem()
				}
				sliceValue.Set(reflect.Append(sliceValue, v))
			} else {
				list = append(list, v.Interface())
			}
		}
	}

	if appendToSlice && sliceValue.IsNil() {
		sliceValue.Set(reflect.MakeSlice(sliceValue.Type(), 0, 0))
	}

	return list, nonFatalErr
}

func handleMultiJoin(v reflect.Value, table *TableMap, multiJoinCols [][]*ColumnMap, parsedRows map[reflect.Type]map[string]reflect.Value) bool {
	rowKeyBuf := bytes.Buffer{}
	rowKeyBuf.WriteRune('|')
	for _, k := range table.keys {
		thisRowKey := v.FieldByIndex(k.fieldIndex).Interface()
		rowKeyBuf.WriteString(fmt.Sprint(thisRowKey))
		rowKeyBuf.WriteRune('|')
	}
	rowKey := rowKeyBuf.String()

	tableValueMap, ok := parsedRows[table.gotype]
	if !ok {
		tableValueMap = make(map[string]reflect.Value)
		parsedRows[table.gotype] = tableValueMap
	}

	rowVal, ok := tableValueMap[rowKey]
	if !ok {
		tableValueMap[rowKey] = v
		return false
	}

	for _, joinedCols := range multiJoinCols {
		subV := v
		targetV := rowVal
		newColFound := false
		keyBuf := rowKeyBuf
		for _, subCol := range joinedCols {
			subV = subV.FieldByIndex(subCol.fieldIndex).Index(0)
			valueMap, ok := parsedRows[subV.Type()]
			if !ok {
				valueMap = make(map[string]reflect.Value)
				parsedRows[subV.Type()] = valueMap
			}
			keyFinder := subV
			if keyFinder.Kind() == reflect.Ptr {
				keyFinder = keyFinder.Elem()
			}
			keyBuf.WriteRune('|')
			for _, k := range subCol.targetTable.keys {
				thisRowKey := keyFinder.FieldByIndex(k.fieldIndex).Interface()
				keyBuf.WriteString(fmt.Sprint(thisRowKey))
				keyBuf.WriteRune('|')
			}
			key := keyBuf.String()

			existingRow, exists := valueMap[key]
			if !exists {
				valueMap[key] = subV
				if newColFound {
					// Sub-elements of a new column will be populated
					// already, so skip appending.
					continue
				}
				targetV = targetV.FieldByIndex(subCol.fieldIndex)
				targetV.Set(reflect.Append(targetV, subV))
				newColFound = true
			}
			targetV = existingRow
		}
	}
	return true
}

// maybeExpandNamedQuery checks the given arg to see if it's eligible to be used
// as input to a named query.  If so, it rewrites the query to use
// dialect-dependent bindvars and instantiates the corresponding slice of
// parameters by extracting data from the map / struct.
// If not, returns the input values unchanged.
func maybeExpandNamedQuery(m *DbMap, query string, args []interface{}) (string, []interface{}) {
	arg := reflect.ValueOf(args[0])
	for arg.Kind() == reflect.Ptr {
		arg = arg.Elem()
	}
	switch {
	case arg.Kind() == reflect.Map && arg.Type().Key().Kind() == reflect.String:
		return expandNamedQuery(m, query, func(key string) reflect.Value {
			return arg.MapIndex(reflect.ValueOf(key))
		})
		// #84 - ignore time.Time structs here - there may be a cleaner way to do this
	case arg.Kind() == reflect.Struct && !(arg.Type().PkgPath() == "time" && arg.Type().Name() == "Time"):
		return expandNamedQuery(m, query, arg.FieldByName)
	}
	return query, args
}

var keyRegexp = regexp.MustCompile(`:[[:word:]]+`)

// expandNamedQuery accepts a query with placeholders of the form ":key", and a
// single arg of Kind Struct or Map[string].  It returns the query with the
// dialect's placeholders, and a slice of args ready for positional insertion
// into the query.
func expandNamedQuery(m *DbMap, query string, keyGetter func(key string) reflect.Value) (string, []interface{}) {
	var (
		n    int
		args []interface{}
	)
	return keyRegexp.ReplaceAllStringFunc(query, func(key string) string {
		val := keyGetter(key[1:])
		if !val.IsValid() {
			return key
		}
		args = append(args, val.Interface())
		newVar := m.Dialect.BindVar(n)
		n++
		return newVar
	}), args
}

func fieldIndexForColumnName(m *DbMap, table *TableMap, t reflect.Type, col string) (fieldIndex []int, joinColumns []*ColumnMap, err error) {
	// Simplest case: the column is mapped to a field in t.
	if table != nil {
		var column *ColumnMap
		joinColumns, column = colMapOrNil(table, col)
		if column != nil {
			fieldIndex = column.fieldIndex
			return
		}
	}
	// Less simple case: the type has no mapping or the column is not
	// mapped to a field
	_, found := t.FieldByNameFunc(func(fieldName string) bool {
		field, _ := t.FieldByName(fieldName)
		fieldName, options := m.columnNameAndOptions(field)

		if fieldName == "-" {
			return false
		}
		for _, option := range options {
			if option == "embed" {
				var index []int
				if index, joinColumns, err = fieldIndexForColumnName(m, table, field.Type, col); err == nil {
					fieldIndex = append(field.Index, index...)
					return true
				}
				return false
			}
		}
		if col == strings.ToLower(fieldName) {
			fieldIndex = field.Index
			return true
		}
		return false
	})
	if !found {
		err = fmt.Errorf("Could not find field for column %s", col)
	}
	return
}

type fieldMap struct {
	index  []int
	gotype reflect.Type
	keys   [][]int
}

func columnToFieldIndex(m *DbMap, t reflect.Type, cols []string) (colToFieldIndex [][]int, table *TableMap, joinMaps [][]*ColumnMap, err error) {
	colToFieldIndex = make([][]int, len(cols))
	joinMaps = make([][]*ColumnMap, 3)

	// check if type t is a mapped table - if so we'll
	// check the table for column aliasing below
	table = tableOrNil(m, t)

	// Loop over column names and find field in i to bind to
	// based on column name. all returned columns must match
	// a field in the i struct
	missingColNames := []string{}
	for x := range cols {
		colName := strings.ToLower(cols[x])
		idx, joinCols, err := fieldIndexForColumnName(m, table, t, colName)
		if err != nil {
			missingColNames = append(missingColNames, colName)
			continue
		}
		colToFieldIndex[x] = idx
		if joinCols != nil {
			alreadyMapped := false
			for _, m := range joinMaps {
				for i := 0; len(m) == len(joinCols) && i < len(m); i++ {
					alreadyMapped = joinCols[i] == m[i]
					if !alreadyMapped {
						break
					}
				}
				if alreadyMapped {
					break
				}
			}
			if !alreadyMapped {
				joinMaps = append(joinMaps, joinCols)
			}
		}
	}
	if len(missingColNames) > 0 {
		err = &NoFieldInTypeError{
			TypeName:        t.Name(),
			MissingColNames: missingColNames,
		}
	}
	return
}

func fieldByName(val reflect.Value, fieldName string) *reflect.Value {
	// try to find field by exact match
	f := val.FieldByName(fieldName)

	if f != zeroVal {
		return &f
	}

	// try to find by case insensitive match - only the Postgres driver
	// seems to require this - in the case where columns are aliased in the sql
	fieldNameL := strings.ToLower(fieldName)
	fieldCount := val.NumField()
	t := val.Type()
	for i := 0; i < fieldCount; i++ {
		sf := t.Field(i)
		if strings.ToLower(sf.Name) == fieldNameL {
			f := val.Field(i)
			return &f
		}
	}

	return nil
}

// toSliceType returns the element type of the given object, if the object is a
// "*[]*Element" or "*[]Element". If not, returns nil.
// err is returned if the user was trying to pass a pointer-to-slice but failed.
func toSliceType(i interface{}) (reflect.Type, error) {
	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		// If it's a slice, return a more helpful error message
		if t.Kind() == reflect.Slice {
			return nil, fmt.Errorf("gorp: Cannot SELECT into a non-pointer slice: %v", t)
		}
		return nil, nil
	}
	if t = t.Elem(); t.Kind() != reflect.Slice {
		return nil, nil
	}
	return t.Elem(), nil
}

func toType(i interface{}) (reflect.Type, error) {
	t := reflect.TypeOf(i)

	// If a Pointer to a type, follow
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("gorp: Cannot SELECT into this type: %v", reflect.TypeOf(i))
	}
	return t, nil
}

func get(m *DbMap, exec SqlExecutor, i interface{},
	keys ...interface{}) (interface{}, error) {

	t, err := toType(i)
	if err != nil {
		return nil, err
	}

	table, err := m.TableFor(t, true)
	if err != nil {
		return nil, err
	}

	plan := table.bindGet()

	v := reflect.New(t)
	dest := make([]interface{}, len(plan.argFields))

	conv := m.TypeConverter
	custScan := make([]binder, 0)

	for x, fieldIdx := range plan.argFields {
		f, bind := fieldByIndex(v, fieldIdx, true)
		if bind != nil {
			custScan = append(custScan, bind)
		}
		target := f.Addr().Interface()
		if conv != nil {
			scanner, ok := conv.FromDb(target)
			if ok {
				if bind != nil {
					// The fieldBinder will call scanner.Bind().
					bind.setScanner(scanner)
				} else {
					target = scanner.Holder
					custScan = append(custScan, scanner)
				}
			}
		}
		if bind != nil {
			target = bind.holder.Addr().Interface()
		}
		dest[x] = target
	}

	row := exec.queryRow(plan.query, keys...)
	err = row.Scan(dest...)
	if err != nil {
		if err == sql.ErrNoRows {
			err = nil
		}
		return nil, err
	}

	for _, c := range custScan {
		err = c.Bind()
		if err != nil {
			return nil, err
		}
	}

	if v, ok := v.Interface().(HasPostGet); ok {
		err := v.PostGet(exec)
		if err != nil {
			return nil, err
		}
	}

	return v.Interface(), nil
}

func delete(m *DbMap, exec SqlExecutor, list ...interface{}) (int64, error) {
	count := int64(0)
	for _, ptr := range list {
		table, elem, err := m.tableForPointer(ptr, true)
		if err != nil {
			return -1, err
		}

		eval := elem.Addr().Interface()
		if v, ok := eval.(HasPreDelete); ok {
			err = v.PreDelete(exec)
			if err != nil {
				return -1, err
			}
		}

		bi, err := table.bindDelete(elem)
		if err != nil {
			return -1, err
		}

		res, err := exec.Exec(bi.query, bi.args...)
		if err != nil {
			return -1, err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return -1, err
		}

		if rows == 0 && bi.existingVersion > 0 {
			return lockError(m, exec, table.TableName,
				bi.existingVersion, elem, bi.keys...)
		}

		count += rows

		if v, ok := eval.(HasPostDelete); ok {
			err := v.PostDelete(exec)
			if err != nil {
				return -1, err
			}
		}
	}

	return count, nil
}

func update(m *DbMap, exec SqlExecutor, list ...interface{}) (int64, error) {
	count := int64(0)
	for _, ptr := range list {
		table, elem, err := m.tableForPointer(ptr, true)
		if err != nil {
			return -1, err
		}

		eval := elem.Addr().Interface()
		if v, ok := eval.(HasPreUpdate); ok {
			err = v.PreUpdate(exec)
			if err != nil {
				return -1, err
			}
		}

		bi, err := table.bindUpdate(elem)
		if err != nil {
			return -1, err
		}

		res, err := exec.Exec(bi.query, bi.args...)
		if err != nil {
			return -1, err
		}

		rows, err := res.RowsAffected()
		if err != nil {
			return -1, err
		}

		if rows == 0 && bi.existingVersion > 0 {
			return lockError(m, exec, table.TableName,
				bi.existingVersion, elem, bi.keys...)
		}

		if bi.versField != nil {
			elem.FieldByIndex(bi.versField).SetInt(bi.existingVersion + 1)
		}

		count += rows

		if v, ok := eval.(HasPostUpdate); ok {
			err = v.PostUpdate(exec)
			if err != nil {
				return -1, err
			}
		}
	}
	return count, nil
}

func insert(m *DbMap, exec SqlExecutor, list ...interface{}) error {
	for _, ptr := range list {
		table, elem, err := m.tableForPointer(ptr, false)
		if err != nil {
			return err
		}

		eval := elem.Addr().Interface()
		if v, ok := eval.(HasPreInsert); ok {
			err := v.PreInsert(exec)
			if err != nil {
				return err
			}
		}

		bi, err := table.bindInsert(elem)
		if err != nil {
			return err
		}

		if bi.autoIncrIdx > -1 {
			f := elem.FieldByIndex(bi.autoIncrFieldIdx)
			switch inserter := m.Dialect.(type) {
			case IntegerAutoIncrInserter:
				id, err := inserter.InsertAutoIncr(exec, bi.query, bi.args...)
				if err != nil {
					return err
				}
				k := f.Kind()
				if (k == reflect.Int) || (k == reflect.Int16) || (k == reflect.Int32) || (k == reflect.Int64) {
					f.SetInt(id)
				} else if (k == reflect.Uint) || (k == reflect.Uint16) || (k == reflect.Uint32) || (k == reflect.Uint64) {
					f.SetUint(uint64(id))
				} else {
					return fmt.Errorf("gorp: Cannot set autoincrement value on non-Int field. SQL=%s  autoIncrIdx=%d autoIncrFieldIdx=%s", bi.query, bi.autoIncrIdx, bi.autoIncrFieldIdx)
				}
			case TargetedAutoIncrInserter:
				err := inserter.InsertAutoIncrToTarget(exec, bi.query, f.Addr().Interface(), bi.args...)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("gorp: Cannot use autoincrement fields on dialects that do not implement an autoincrementing interface")
			}
		} else {
			_, err := exec.Exec(bi.query, bi.args...)
			if err != nil {
				return err
			}
		}

		if v, ok := eval.(HasPostInsert); ok {
			err := v.PostInsert(exec)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func lockError(m *DbMap, exec SqlExecutor, tableName string,
	existingVer int64, elem reflect.Value,
	keys ...interface{}) (int64, error) {

	existing, err := get(m, exec, elem.Interface(), keys...)
	if err != nil {
		return -1, err
	}

	ole := OptimisticLockError{tableName, keys, true, existingVer}
	if existing == nil {
		ole.RowExists = false
	}
	return -1, ole
}

// PostUpdate() will be executed after the GET statement.
type HasPostGet interface {
	PostGet(SqlExecutor) error
}

// PostUpdate() will be executed after the DELETE statement
type HasPostDelete interface {
	PostDelete(SqlExecutor) error
}

// PostUpdate() will be executed after the UPDATE statement
type HasPostUpdate interface {
	PostUpdate(SqlExecutor) error
}

// PostInsert() will be executed after the INSERT statement
type HasPostInsert interface {
	PostInsert(SqlExecutor) error
}

// PreDelete() will be executed before the DELETE statement.
type HasPreDelete interface {
	PreDelete(SqlExecutor) error
}

// PreUpdate() will be executed before UPDATE statement.
type HasPreUpdate interface {
	PreUpdate(SqlExecutor) error
}

// PreInsert() will be executed before INSERT statement.
type HasPreInsert interface {
	PreInsert(SqlExecutor) error
}
