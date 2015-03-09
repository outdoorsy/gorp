package gorp

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// A TypeDefSwitcher is a TypeDeffer except that it won't have its type
// defined on table creation.  Instead, its type will be changed on
// the first migration run.  Useful for foreign keys when the target
// table may be created later than the column.
type TypeDefSwitcher interface {
	// TypeDefSwitch should return the same thing as
	// TypeDeffer.TypeDef.
	TypeDefSwitch() string
}

// A TypeCaster includes TypeDeffer logic but can also return the SQL
// to cast old types to its new type.
type TypeCaster interface {
	// TypeCast should return a string with zero or one '%s'
	// sequences.  If the string contains a '%s', it will be replaced
	// with the old value; otherwise, the return value will simply be
	// used as the type to cast to in the database.
	TypeCast() string
}

type pgJSON []byte

func (j pgJSON) Value() (driver.Value, error) {
	return []byte(j), nil
}

func (j *pgJSON) Scan(src interface{}) error {
	var source []byte
	switch src.(type) {
	case string:
		source = []byte(src.(string))
	case []byte:
		source = src.([]byte)
	default:
		return errors.New("Incompatible type for pgJSON")
	}
	*j = pgJSON(append((*j)[0:0], source...))
	return nil
}

func (j pgJSON) TypeDef() string {
	return "json"
}

type columnLayout struct {
	FieldName  string `json:"field_name"`
	ColumnName string `json:"column_name"`
	TypeDef    string `json:"type_def"`

	// Values that are only used on the new layout, but are
	// unnecessary for old types.
	isTypeSwitch bool         `json:"-"`
	gotype       reflect.Type `json:"-"`
	typeCast     string       `json:"-"`
}

type tableRecord struct {
	ID          int
	Schemaname  string
	Tablename   string
	Layout      pgJSON
	tableLayout []columnLayout `db:"-"`
}

func (t *tableRecord) TableLayout() []columnLayout {
	if t.tableLayout == nil {
		t.tableLayout = []columnLayout{}
		if err := json.Unmarshal([]byte(t.Layout), &t.tableLayout); err != nil {
			panic(err)
		}
	}
	return t.tableLayout
}

func (t *tableRecord) SetTableLayout(l []columnLayout) {
	t.tableLayout = l
	b, err := json.Marshal(l)
	if err != nil {
		panic(err)
	}
	t.Layout = pgJSON(b)
}

func (t *tableRecord) Merge(l []columnLayout) {
	if t.tableLayout == nil {
		t.SetTableLayout(l)
		return
	}
	for _, newCol := range l {
		shouldAppend := true
		for _, oldCol := range t.tableLayout {
			if newCol.ColumnName == oldCol.ColumnName {
				shouldAppend = false
			}
		}
		if shouldAppend {
			t.tableLayout = append(t.tableLayout, newCol)
		}
	}
}

func ptrToVal(t reflect.Type) reflect.Value {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return reflect.New(t)
}

type MigrationManager struct {
	schemaname string
	tablename  string
	dbMap      *DbMap
	oldTables  []*tableRecord
	newTables  []*tableRecord
}

func (m *MigrationManager) layoutFor(t *TableMap) []columnLayout {
	l := make([]columnLayout, 0, len(t.Columns))
	for _, colMap := range t.Columns {
		if colMap.ColumnName == "-" {
			continue
		}
		var stype string
		orig := ptrToVal(colMap.origtype).Interface()
		dbValue := ptrToVal(colMap.gotype).Interface()
		typer, hasDef := orig.(TypeDeffer)
		if !hasDef && colMap.origtype != colMap.gotype {
			typer, hasDef = dbValue.(TypeDeffer)
		}
		typeSwitcher, hasSwitch := orig.(TypeDefSwitcher)
		if !hasSwitch && colMap.origtype != colMap.gotype {
			typeSwitcher, hasSwitch = dbValue.(TypeDefSwitcher)
		}
		if hasDef {
			stype = typer.TypeDef()
		} else if hasSwitch {
			stype = typeSwitcher.TypeDefSwitch()
		} else {
			stype = m.dbMap.Dialect.ToSqlType(colMap.gotype, colMap.MaxSize, colMap.isAutoIncr)
		}
		cast := "%s"
		typeCaster, ok := orig.(TypeCaster)
		if !ok {
			typeCaster, ok = dbValue.(TypeCaster)
		}
		if ok {
			cast = typeCaster.TypeCast()
			if !strings.Contains(cast, "%s") {
				cast = "%s::" + cast
			}
		}

		col := columnLayout{
			FieldName:    colMap.fieldName,
			ColumnName:   colMap.ColumnName,
			TypeDef:      stype,
			isTypeSwitch: hasSwitch,
			typeCast:     cast,
			gotype:       colMap.gotype,
		}
		l = append(l, col)
	}
	return l
}

func (m *MigrationManager) addTable(t *TableMap) {
	l := m.layoutFor(t)
	for _, r := range m.newTables {
		if r.Schemaname == t.SchemaName && r.Tablename == t.TableName {
			r.Merge(l)
			return
		}
	}
	r := &tableRecord{
		Schemaname: t.SchemaName,
		Tablename:  t.TableName,
	}
	r.SetTableLayout(l)
	m.newTables = append(m.newTables, r)
}

func (m *MigrationManager) newTableRecords() []*tableRecord {
	if m.newTables == nil {
		m.newTables = make([]*tableRecord, 0, len(m.dbMap.tables))
		for _, tableMap := range m.dbMap.tables {
			m.addTable(tableMap)
		}
	}
	return m.newTables
}

func (m *MigrationManager) Migrate() (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch src := r.(type) {
			case error:
				err = src
			default:
				err = fmt.Errorf("Recovered from panic: %v", src)
			}
		}
	}()
	quotedTable := m.dbMap.Dialect.QuotedTableForQuery(m.schemaname, m.tablename)
	_, err = m.dbMap.Select(&m.oldTables, "select * from "+quotedTable)
	if err != nil {
		return err
	}
	return m.run()
}

func (m *MigrationManager) run() error {
	for _, newTable := range m.newTableRecords() {
		found := false
		for _, oldTable := range m.oldTables {
			if oldTable.Schemaname == newTable.Schemaname && oldTable.Tablename == newTable.Tablename {
				found = true
				if err := m.migrateTable(oldTable, newTable); err != nil {
					return err
				}
			}
		}
		if !found {
			if err := m.handleTypeSwitches(newTable); err != nil {
				return err
			}
			if err := m.dbMap.Insert(newTable); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *MigrationManager) handleTypeSwitches(table *tableRecord) (err error) {
	tx, err := m.dbMap.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				panic(rollbackErr)
			}
		}
	}()
	quotedTable := m.dbMap.Dialect.QuotedTableForQuery(table.Schemaname, table.Tablename)
	for _, newCol := range table.TableLayout() {
		if newCol.isTypeSwitch {
			if err = m.changeType(quotedTable, newCol, tx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *MigrationManager) changeType(quotedTable string, newCol columnLayout, tx *Transaction) error {
	quotedColumn := m.dbMap.Dialect.QuoteField(newCol.ColumnName)
	oldQuotedColumn := m.dbMap.Dialect.QuoteField(newCol.ColumnName + "_type_change_bak")
	sql := "ALTER TABLE " + quotedTable + " RENAME COLUMN " + quotedColumn + " TO " + oldQuotedColumn
	if _, err := tx.Exec(sql); err != nil {
		return err
	}
	sql = "ALTER TABLE " + quotedTable + " ADD COLUMN " + quotedColumn + " " + newCol.TypeDef
	if _, err := tx.Exec(sql); err != nil {
		return err
	}
	sql = "UPDATE " + quotedTable + " SET " + quotedColumn + " = " + fmt.Sprintf(newCol.typeCast, oldQuotedColumn)
	if _, err := tx.Exec(sql); err != nil {
		return err
	}
	sql = "ALTER TABLE " + quotedTable + " DROP COLUMN " + oldQuotedColumn
	if _, err := tx.Exec(sql); err != nil {
		return err
	}
	return nil
}

func (m *MigrationManager) migrateTable(oldTable, newTable *tableRecord) (err error) {
	tx, err := m.dbMap.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				panic(rollbackErr)
			}
		}
	}()
	if oldTable.Schemaname != newTable.Schemaname || oldTable.Tablename != newTable.Tablename {
		return fmt.Errorf("Unsupported operation: table name change (%s.%s to %s.%s)",
			oldTable.Schemaname,
			oldTable.Tablename,
			newTable.Schemaname,
			newTable.Tablename,
		)
	}
	quotedTable := m.dbMap.Dialect.QuotedTableForQuery(newTable.Schemaname, newTable.Tablename)
	for _, newCol := range newTable.TableLayout() {
		quotedColumn := m.dbMap.Dialect.QuoteField(newCol.ColumnName)
		found := false
		for _, oldCol := range oldTable.TableLayout() {
			if strings.ToLower(oldCol.ColumnName) == strings.ToLower(newCol.ColumnName) {
				found = true
				if oldCol.TypeDef != newCol.TypeDef {
					if err := m.changeType(quotedTable, newCol, tx); err != nil {
						return err
					}
				}
				break
			}
			if oldCol.FieldName == newCol.FieldName {
				found = true
				oldQuotedColumn := m.dbMap.Dialect.QuoteField(oldCol.ColumnName)
				sql := "ALTER TABLE " + quotedTable + " RENAME COLUMN " + oldQuotedColumn + " TO " + quotedColumn
				if _, err := tx.Exec(sql); err != nil {
					return err
				}
				break
			}
		}
		if !found {
			sql := "ALTER TABLE " + quotedTable + " ADD COLUMN " + quotedColumn + " " + newCol.TypeDef
			if _, err := tx.Exec(sql); err != nil {
				return err
			}
			defaultVal := reflect.New(newCol.gotype).Interface()
			sql = "UPDATE " + quotedTable + " SET " + quotedColumn + "=" + m.dbMap.Dialect.BindVar(0)
			if _, err := tx.Exec(sql, defaultVal); err != nil {
				return err
			}
		}
	}
	newTable.ID = oldTable.ID
	if count, err := tx.Update(newTable); err != nil || count != 1 {
		return err
	}
	return nil
}

// Migrater returns a MigrationManager using the given tablename as
// the migration table.
func (m *DbMap) Migrater(schemaname, tablename string) (*MigrationManager, error) {
	// Just run the create table statement for the migration table,
	// using a temporary DbMap
	tmpM := &DbMap{
		Db:      m.Db,
		Dialect: m.Dialect,
	}
	tmpM.AddTableWithNameAndSchema(tableRecord{}, schemaname, tablename).SetKeys(true, "ID")
	if err := tmpM.CreateTablesIfNotExists(); err != nil {
		return nil, err
	}
	m.AddTableWithNameAndSchema(tableRecord{}, schemaname, tablename).SetKeys(true, "ID")
	return &MigrationManager{
		schemaname: schemaname,
		tablename:  tablename,
		dbMap:      m,
	}, nil
}