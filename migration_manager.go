package gorp

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
)

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

	// Used to ensure that we don't get scan errors on new columns.
	// We'll probably want to use this for type changes as well
	// (e.g. changing from a *string to a string, we will need to
	// update all NULL values to "")
	gotype reflect.Type `json:"-"`
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
		typer, ok := reflect.New(colMap.origtype).Interface().(TypeDeffer)
		if !ok && colMap.origtype != colMap.gotype {
			typer, ok = reflect.New(colMap.gotype).Interface().(TypeDeffer)
		}
		if ok {
			stype = typer.TypeDef()
		} else {
			stype = m.dbMap.Dialect.ToSqlType(colMap.gotype, colMap.MaxSize, colMap.isAutoIncr)
		}

		col := columnLayout{
			FieldName:  colMap.fieldName,
			ColumnName: colMap.ColumnName,
			TypeDef:    stype,
			gotype:     colMap.gotype,
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
			l = nil
		}
	}
	if l != nil {
		r := &tableRecord{
			Schemaname: t.SchemaName,
			Tablename:  t.TableName,
		}
		r.SetTableLayout(l)
		m.newTables = append(m.newTables, r)
	}
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
		log.Print("Migration error:", err)
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
			if err := m.dbMap.Insert(newTable); err != nil {
				return err
			}
		}
	}
	return nil
}

// layout:
//
// [
//   {
//     "field_name": someName,
//     "column_name": someOtherName,
//     "type_def": someDefinition,
//     "constraints": [],
//   }
// ]
func (m *MigrationManager) migrateTable(oldTable, newTable *tableRecord) (err error) {
	tx, err := m.dbMap.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		if err == nil {
			log.Printf("Error is nil, committing")
			err = tx.Commit()
		} else {
			log.Printf("Error is non-nil, rolling back: %v", err)
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
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
	for _, newCol := range newTable.TableLayout() {
		found := false
		for _, oldCol := range oldTable.TableLayout() {
			if oldCol.ColumnName == newCol.ColumnName {
				found = true
				if oldCol.TypeDef != newCol.TypeDef {
					return errors.New("Unsupported operation: type definition change")
				}
			} else if oldCol.FieldName == newCol.FieldName {
				return errors.New("Unsupported operation: column name change")
			}
		}
		if !found {
			quotedTable := m.dbMap.Dialect.QuotedTableForQuery(newTable.Schemaname, newTable.Tablename)
			quotedColumn := m.dbMap.Dialect.QuoteField(newCol.ColumnName)
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
	m.TraceOn("DB:", log.New(os.Stdout, "", log.LstdFlags))
	m.AddTableWithNameAndSchema(tableRecord{}, schemaname, tablename).SetKeys(true, "ID")
	return &MigrationManager{
		schemaname: schemaname,
		tablename:  tablename,
		dbMap:      m,
	}, nil
}
