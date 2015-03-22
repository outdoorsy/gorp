package gorp

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

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
	OldTypeDef string `json:"type_def"`
	TypeDef    string `json:"type_def_v2"`
	IsNotNull  bool   `json:"is_not_null"`

	// Values that are only used on the new layout, but are
	// unnecessary for old types.
	isPK         bool         `json:"-"`
	hasReference bool         `json:"-"`
	gotype       reflect.Type `json:"-"`
	typeCast     string       `json:"-"`
	oldFieldName string       `json:"-"`
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

// typeDefSwitcher is a deprecated interface used in an old version of
// the migration manager.
type typeDefSwitcher interface {
	// TypeDefSwitch should return the same thing as
	// TypeDeffer.TypeDef.
	TypeDefSwitch() string
}

// deprecatedTypeDef is the old, now-deprecated TypeDef format.  We
// want to keep it updated for a time, for rolling back purposes.
func (m *MigrationManager) deprecatedTypeDef(colMap *ColumnMap) string {
	var stype string
	orig := ptrToVal(colMap.origtype).Interface()
	dbValue := ptrToVal(colMap.gotype).Interface()
	typer, hasDef := orig.(TypeDeffer)
	if !hasDef && colMap.origtype != colMap.gotype {
		typer, hasDef = dbValue.(TypeDeffer)
	}
	typeSwitcher, hasSwitch := orig.(typeDefSwitcher)
	if !hasSwitch && colMap.origtype != colMap.gotype {
		typeSwitcher, hasSwitch = dbValue.(typeDefSwitcher)
	}
	if hasDef {
		stype = typer.TypeDef()
	} else if hasSwitch {
		stype = typeSwitcher.TypeDefSwitch()
	} else {
		stype = m.dbMap.Dialect.ToSqlType(colMap.gotype, colMap.MaxSize, colMap.isAutoIncr)
	}
	return stype
}

func (m *MigrationManager) layoutFor(t *TableMap) []columnLayout {
	l := make([]columnLayout, 0, len(t.Columns))
	for _, colMap := range t.Columns {
		if colMap.Transient {
			continue
		}
		stype, notNullIgnored := colMap.TypeDefNoNotNull()
		orig := ptrToVal(colMap.origtype).Interface()
		dbValue := ptrToVal(colMap.gotype).Interface()
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
			OldTypeDef:   m.deprecatedTypeDef(colMap),
			IsNotNull:    notNullIgnored,
			isPK:         colMap.isPK,
			hasReference: colMap.References() != nil,
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
	tx, err := m.dbMap.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		if r := recover(); r != nil {
			switch src := r.(type) {
			case error:
				err = src
			default:
				err = fmt.Errorf("Recovered from panic: %v", src)
			}
		}
		if err == nil {
			err = tx.Commit()
		} else {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				panic(rollbackErr)
			}
		}
	}()
	quotedTable := m.dbMap.Dialect.QuotedTableForQuery(m.schemaname, m.tablename)
	_, err = m.dbMap.Select(&m.oldTables, "select * from "+quotedTable)
	if err != nil {
		return err
	}
	// Check the old table layout for missing current versions
	for _, t := range m.oldTables {
		for _, oldCol := range t.tableLayout {
			if oldCol.TypeDef == "" {
				quotedColumn := m.dbMap.Dialect.QuoteField(oldCol.ColumnName)
				oldCol.TypeDef = quotedColumn + " " + oldCol.OldTypeDef
			}
		}
	}
	return m.run(tx)
}

func (m *MigrationManager) run(tx *Transaction) error {
	for _, newTable := range m.newTableRecords() {
		found := false
		for _, oldTable := range m.oldTables {
			if oldTable.Schemaname == newTable.Schemaname && oldTable.Tablename == newTable.Tablename {
				found = true
				if err := m.migrateTable(oldTable, newTable, tx); err != nil {
					return err
				}
			}
		}
		if !found {
			if err := tx.Insert(newTable); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *MigrationManager) changeType(quotedTable string, newCol, oldCol columnLayout, tx *Transaction) error {
	if newCol.isPK {
		// Ehrm.  Backward compatibility issue, here.  Just ignore
		// PKeys for now.
		return nil
	}
	quotedNewCol := m.dbMap.Dialect.QuoteField(newCol.ColumnName)
	quotedOldCol := m.dbMap.Dialect.QuoteField(oldCol.ColumnName)
	quotedBakCol := quotedOldCol
	if quotedOldCol == quotedNewCol {
		quotedBakCol = m.dbMap.Dialect.QuoteField(oldCol.ColumnName + "_type_change_bak")
		sql := "ALTER TABLE " + quotedTable + " RENAME COLUMN " + quotedOldCol + " TO " + quotedBakCol
		if _, err := tx.Exec(sql); err != nil {
			return err
		}
	}
	sql := "ALTER TABLE " + quotedTable + " ADD COLUMN " + newCol.TypeDef
	if _, err := tx.Exec(sql); err != nil {
		return err
	}
	sql = "UPDATE " + quotedTable + " SET " + quotedNewCol + " = " + fmt.Sprintf(newCol.typeCast, quotedBakCol)
	if _, err := tx.Exec(sql); err != nil {
		return err
	}
	if newCol.IsNotNull && oldCol.IsNotNull {
		// If the not null setting has changed, it will be handled
		// elsewhere; but if it is set to true on both the old column
		// and the new column, we have to handle it here.
		sql = "ALTER TABLE " + quotedTable + " ALTER COLUMN " + quotedNewCol + " SET NOT NULL"
		if _, err := tx.Exec(sql); err != nil {
			return err
		}
	}
	sql = "ALTER TABLE " + quotedTable + " DROP COLUMN " + quotedBakCol
	if _, err := tx.Exec(sql); err != nil {
		return err
	}
	return nil
}

func (m *MigrationManager) migrateTable(oldTable, newTable *tableRecord, tx *Transaction) (err error) {
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
			found = strings.ToLower(oldCol.ColumnName) == strings.ToLower(newCol.ColumnName) ||
				oldCol.FieldName != "" && oldCol.FieldName == newCol.FieldName
			if !found {
				// This *only* handles conversion from the old foreign
				// key structure to the new one.
				found = strings.HasPrefix(newCol.FieldName, oldCol.FieldName+".")
			}
			if found {
				if oldCol.TypeDef != newCol.TypeDef {
					if err := m.changeType(quotedTable, newCol, oldCol, tx); err != nil {
						return err
					}
				}
				notNullOp := ""
				if !oldCol.IsNotNull && newCol.IsNotNull {
					notNullOp = "SET"
				} else if oldCol.IsNotNull && !newCol.IsNotNull {
					notNullOp = "DROP"
				}
				if notNullOp != "" {
					sql := "ALTER TABLE " + quotedTable + " ALTER COLUMN " + quotedColumn + " " + notNullOp + " NOT NULL"
					if _, err := tx.Exec(sql); err != nil {
						return err
					}
				}
				break
			}
		}
		if !found {
			sql := "ALTER TABLE " + quotedTable + " ADD COLUMN " + newCol.TypeDef
			if _, err := tx.Exec(sql); err != nil {
				return err
			}
			// As of the time of this writing, we don't have a way to
			// generate non-nil data for a foreign key column.  If it
			// was set to be NOT NULL, then the next operation (adding
			// the NOT NULL constraint) will fail.
			if !newCol.hasReference {
				defaultVal := reflect.New(newCol.gotype).Interface()
				sql = "UPDATE " + quotedTable + " SET " + quotedColumn + "=" + m.dbMap.Dialect.BindVar(0)
				if _, err := tx.Exec(sql, defaultVal); err != nil {
					return err
				}
			}
			if newCol.IsNotNull {
				// Most likely, the default value above was not null.
				//
				// TODO: support data binding, for when this new
				// column can be populated with data from existing
				// columns or some other method.
				sql = "ALTER " + quotedTable + " ALTER COLUMN " + quotedColumn + " SET NOT NULL"
				if _, err := tx.Exec(sql); err != nil {
					return err
				}
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
	added := false
	for _, t := range m.tables {
		if t.SchemaName == schemaname && t.TableName == tablename && t.gotype == reflect.TypeOf(tableRecord{}) {
			added = true
			break
		}
	}
	if !added {
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
	}
	return &MigrationManager{
		schemaname: schemaname,
		tablename:  tablename,
		dbMap:      m,
	}, nil
}
