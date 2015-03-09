package gorp

import "testing"

type BasicLeft struct {
	ID     int
	Name   string
	Foo    string
	Rights []*BasicRight `db:"-,fkey,join=basic_right_"`
}

type BasicRight struct {
	ID   int
	Name string
	Bar  string
	Left *BasicLeft `db:",fkey,join=basic_left_"`
}

type MtoMOne struct {
	ID   int
	Name string
	Twos []*MtoMTwo `db:"-,fkey"`
}

type MtoMTwo struct {
	ID   int
	Name string
	Ones []*MtoMOne `db:"-,fkey"`
}

type MtoMThree struct {
	ID   int
	Name string
}

type MtoMThreeMapper struct {
	MapID     int `db:"id"`
	MtoMThree `db:",fkey,join=m_to_m_three_"`
	One       *MtoMOne `db:",fkey,join=m_to_m_one_"`
	Rank      int
}

// dbmap.AddTableWithName(BasicLeft{}, "basic_left").SetKeys(true, "ID")
// r := BasicRight{}
// dbmap.AddTableWithName(&r, "basic_right").SetKeys(true, &r.ID)
// mainTable := dbmap.AddTableWithName(MtoMOne{}, "many_to_many_one").SetKeys(true, "ID")
// dbmap.AddTableWithName(MtoMTwo{}, "many_to_many_two").SetKeys(true, "ID")
// mainTable.ManyToMany(MtoMTwo{})
// mainTable.ManyToManyWithName(MtoMThreeMapper{}, "one_three_map").SetKeys(true, "MapID")
// dbmap.AddTableWithName(MtoMOne{}, "many_to_many_one").SetKeys(true, "ID")

func TestBasicForeignKeySimpleGet(t *testing.T) {
	dbmap := initDbMap()
	defer dropAndClose(dbmap)

	l := &BasicLeft{}
	err := dbmap.Insert(l)
	if err != nil {
		t.Errorf("Error is non-nil: %s", err)
	}

	r := &BasicRight{Left: l}
	err = dbmap.Insert(r)
	if err != nil {
		t.Fatalf("Error is non-nil: %s", err)
	}

	// A normal get should still load the ID
	result, err := dbmap.Get(&BasicRight{}, r.ID)
	if err != nil {
		t.Fatalf("Error is non-nil: %s", err)
	}
	loadedR, ok := result.(*BasicRight)
	if !ok {
		t.Fatalf("Expected result to be a BasicRight pointer")
	}
	if loadedR.ID != r.ID {
		t.Errorf("%v != %v", loadedR.ID, r.ID)
	}
	if loadedR.Left == nil {
		t.Fatalf("Expected Left to be loaded")
	}
	if loadedR.Left.ID != l.ID {
		t.Errorf("%v != %v", loadedR.Left.ID, l.ID)
	}
}

func TestBasicForeignKeyMultiSelect(t *testing.T) {
	dbmap := initDbMap()
	//defer dropAndClose(dbmap)

	l := &BasicLeft{
		Name: "foo",
		Foo:  "this is definitely a foo",
	}
	err := dbmap.Insert(l)
	if err != nil {
		t.Errorf("Error is non-nil: %s", err)
	}

	r := &BasicRight{
		Name: "bar",
		Bar:  "this is definitely a bar",
		Left: l,
	}
	err = dbmap.Insert(r)
	if err != nil {
		t.Fatalf("Error is non-nil: %s", err)
	}

	sql := `select basic_right.id, basic_right.name, basic_right.bar, ` +
		`basic_left.id as basic_left_id, basic_left.name as basic_left_name, basic_left.foo as basic_left_foo ` +
		"from basic_left " +
		"inner join basic_right on basic_left.id = basic_right.basic_left_id " +
		"where basic_right.id = " + dbmap.Dialect.BindVar(0)
	results, err := dbmap.Select(&BasicRight{}, sql, r.ID)
	if err != nil {
		t.Fatalf("Error is non-nil: %s", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected exactly one result for ID %d", r.ID)
	}

	loadedR, ok := results[0].(*BasicRight)
	if !ok {
		t.Fatalf("Expected result to be a BasicRight pointer")
	}
	if loadedR.ID != r.ID {
		t.Errorf("%v != %v", loadedR.ID, r.ID)
	}
	if loadedR.Name != r.Name {
		t.Errorf("%v != %v", loadedR.Name, r.Name)
	}
	if loadedR.Bar != r.Bar {
		t.Errorf("%v != %v", loadedR.Bar, r.Bar)
	}
	if loadedR.Left == nil {
		t.Fatalf("Expected Left to be loaded")
	}
	if loadedR.Left.ID != l.ID {
		t.Errorf("%v != %v", loadedR.Left.ID, l.ID)
	}
	if loadedR.Left.Name != l.Name {
		t.Errorf("%v != %v", loadedR.Left.Name, l.Name)
	}
	if loadedR.Left.Foo != l.Foo {
		t.Errorf("%v != %v", loadedR.Left.Foo, l.Foo)
	}
}

func TestBasicForeignKeyConsolidatedMultiSelect(t *testing.T) {
	dbmap := initDbMap()
	//defer dropAndClose(dbmap)

	l := &BasicLeft{
		Name: "foo",
		Foo:  "this is definitely a foo",
	}
	err := dbmap.Insert(l)
	if err != nil {
		t.Errorf("Error is non-nil: %s", err)
	}

	r1 := &BasicRight{
		Name: "bar 1",
		Bar:  "this is definitely a bar",
		Left: l,
	}
	err = dbmap.Insert(r1)
	if err != nil {
		t.Fatalf("Error is non-nil: %s", err)
	}

	r2 := &BasicRight{
		Name: "bar 2",
		Bar:  "this is definitely another bar",
		Left: l,
	}
	err = dbmap.Insert(r2)
	if err != nil {
		t.Fatalf("Error is non-nil: %s", err)
	}

	sql := `select basic_left.id, basic_left.name, basic_left.foo, ` +
		`basic_right.id as basic_right_id, basic_right.name as basic_right_name, basic_right.bar as basic_right_bar ` +
		"from basic_left " +
		"inner join basic_right on basic_left.id = basic_right.basic_left_id " +
		"where basic_left.id = " + dbmap.Dialect.BindVar(0)
	results, err := dbmap.Select(&BasicLeft{}, sql, l.ID)
	if err != nil {
		t.Fatalf("Error is non-nil: %s", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected exactly one result for ID %d", l.ID)
	}
	loadedL := results[0].(*BasicLeft)
	if len(loadedL.Rights) != 2 {
		t.Errorf("Expected exactly two rights for left ID %d", l.ID)
	}
}
