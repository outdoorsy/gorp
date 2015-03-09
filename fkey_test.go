package gorp

import "testing"

type BasicLeft struct {
	ID     int
	Name   string
	Rights []*BasicRight `db:"-,fkey"`
}

type BasicRight struct {
	ID   int
	Name string
	Left *BasicLeft `db:",fkey"`
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
	MtoMThree `db:",fkey"`
	One       *MtoMOne `db:",fkey"`
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

	sql := `select basic_right.id, basic_right.name, basic_left.id, basic_left.name ` +
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
	if loadedR.Left == nil {
		t.Fatalf("Expected Left to be loaded")
	}
	if loadedR.Left.ID != l.ID {
		t.Errorf("%v != %v", loadedR.Left.ID, l.ID)
	}
}
