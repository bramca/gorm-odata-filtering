# 🔎 gorm-odata-filtering

[![Go package](https://github.com/bramca/gorm-odata-filtering/actions/workflows/test.yaml/badge.svg)](https://github.com/bramca/gorm-odata-filtering/actions/workflows/test.yaml)
![GitHub](https://img.shields.io/github/license/bramca/gorm-odata-filtering)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/bramca/gorm-odata-filtering)

This package provides a way to filter [gorm](https://gorm.io) objects with an [OData](https://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part2-url-conventions/odata-v4.0-errata03-os-part2-url-conventions-complete.html#_Toc453752358) filter.
It builds the correct gorm query based on an odata filter string.
<br>
It creates a `Syntax Tree` based on the input query string using [go-syntax-tree](https://github.com/bramca/go-syntax-tree) and uses that tree to build the correct gorm query.
<br>
To make sure that object expansion works (e.g. `metadata/name eq 'some-value'`) it makes use of the following 2 dependencies:
- [deepgorm](github.com/survivorbat/gorm-deep-filtering)
- [gormqonvert](github.com/survivorbat/gorm-query-convert)

## 📋 Example

``` go
package main

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	gormodata "github.com/bramca/gorm-odata-filtering"
)

type MockModel struct {
	ID        string
	Name      string
	TestValue string
	Metadata   *Metadata `gorm:"foreignKey:MetadataID"`
	MetadataID *uuid.UUID
}

type Metadata struct {
	ID   uuid.UUID
	Name string
}

func main() {
	db, _ := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	db.AutoMigrate(&MockModel{}, &Metadata{})

	queryString := "name eq 'test' and (contains(testValue,'testvalue') or contains(metadata/name,'test-metadata'))"

	var result []MockModel

	// Without validation
	dbQuery, err := gormodata.BuildQuery(queryString, db, gormodata.SQLite)

	if err != nil {
		panic(err)
	}

	dbQuery.Find(&result)

	// With validation
	maxTreeDepth = 7
	dbQuery, err = gormodata.BuildQueryWithValidation(query, db, gormodata.SQLite, MockModel{}, maxTreeDepth)

	if err != nil {
		panic(err)
	}

	dbQuery.Find(&result)
}

```
