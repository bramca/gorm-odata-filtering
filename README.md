# 🔎 gorm-odata-filtering

[![Go package](https://github.com/bramca/gorm-odata-filtering/actions/workflows/test.yaml/badge.svg)](https://github.com/bramca/gorm-odata-filtering/actions/workflows/test.yaml)
![GitHub](https://img.shields.io/github/license/bramca/gorm-odata-filtering)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/bramca/gorm-odata-filtering)

This package provides a way to filter [gorm](https://gorm.io) objects with an [OData](https://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part2-url-conventions/odata-v4.0-errata03-os-part2-url-conventions-complete.html#_Toc453752358) filter.
It builds the correct gorm query based on an odata filter string.

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
}

func main() {
	db, _ := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	db.AutoMigrate(&MockModel{})

	odataQueryBuilder := gormodata.NewOdataQueryBuilder()

	queryString := "name eq 'test' and (contains(testValue,'testvalue') or contains(testValue,'accvalue'))"

	var result []MockModel
	dbQuery, err := odataQueryBuilder.BuildQuery(queryString, db)

	if err != nil {
		panic(err)
	}

	dbQuery.Find(&result)
}

```