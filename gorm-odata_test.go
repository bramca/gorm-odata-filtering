package gormodata

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/ing-bank/gormtestutil"
	"github.com/test-go/testify/assert"
	"gorm.io/gorm"
)

// Mocks
type MockModel struct {
	ID         uuid.UUID
	Name       string
	TestValue  string
	Metadata   *Metadata `gorm:"foreignKey:MetadataID"`
	MetadataID *uuid.UUID
}

type Metadata struct {
	ID          uuid.UUID
	Name        string
	MockModelID uuid.UUID
	MockModel   *MockModel `gorm:"foreignKey:MockModelID"`
}

func Test_BuildQuery_Success(t *testing.T) {
	tests := map[string]struct {
		records        []*MockModel
		queryString    string
		expectedSql    string
		expectedResult []MockModel
	}{
		"simple query": {
			records: []*MockModel{
				{
					ID:        uuid.MustParse("885b50a8-f2d2-4fc2-b8e8-4db54f5ef5b6"),
					Name:      "test",
					TestValue: "prdvalue",
				},
				{
					ID:        uuid.MustParse("d8c9b566-f711-4113-8a86-a07fa470e43a"),
					Name:      "prd",
					TestValue: "accvalue",
				},
				{
					ID:        uuid.MustParse("87e8ed33-512d-4482-b639-e0830a19b653"),
					Name:      "test",
					TestValue: "prdvalue",
				},
				{
					ID:        uuid.MustParse("96954f52-f87c-4ec2-9af5-3e13642bdc83"),
					Name:      "test",
					TestValue: "some-testvalue-1",
				},
				{
					ID:        uuid.MustParse("eab8118c-45e9-4848-a380-ed6d981f2338"),
					Name:      "test",
					TestValue: "someaccvalue",
				},
			},
			queryString: "name ne 'prd' and (contains(testValue,'testvalue') or endswith(testValue,'accvalue'))",
			expectedSql: "SELECT * FROM `mock_models` WHERE name != 'prd' AND (test_value LIKE '%testvalue%' OR test_value LIKE '%accvalue')",
			expectedResult: []MockModel{
				{
					ID:        uuid.MustParse("96954f52-f87c-4ec2-9af5-3e13642bdc83"),
					Name:      "test",
					TestValue: "some-testvalue-1",
				},
				{
					ID:        uuid.MustParse("eab8118c-45e9-4848-a380-ed6d981f2338"),
					Name:      "test",
					TestValue: "someaccvalue",
				},
			},
		},
		"simple query unary function chain": {
			records: []*MockModel{
				{
					ID:        uuid.MustParse("885b50a8-f2d2-4fc2-b8e8-4db54f5ef5b6"),
					Name:      "test",
					TestValue: "prdvalue",
				},
				{
					ID:        uuid.MustParse("d8c9b566-f711-4113-8a86-a07fa470e43a"),
					Name:      "prd",
					TestValue: "accvalue",
				},
				{
					ID:        uuid.MustParse("87e8ed33-512d-4482-b639-e0830a19b653"),
					Name:      "test",
					TestValue: "prdvalue",
				},
				{
					ID:        uuid.MustParse("96954f52-f87c-4ec2-9af5-3e13642bdc83"),
					Name:      "test",
					TestValue: "some-testvalue-1",
				},
				{
					ID:        uuid.MustParse("eab8118c-45e9-4848-a380-ed6d981f2338"),
					Name:      "test",
					TestValue: "someaccvalue",
				},
			},
			queryString: "length(trim(toupper(testValue))) gt 10",
			expectedSql: "SELECT * FROM `mock_models` WHERE LENGTH(TRIM(UPPER(test_value))) > 10",
			expectedResult: []MockModel{
				{
					ID:        uuid.MustParse("96954f52-f87c-4ec2-9af5-3e13642bdc83"),
					Name:      "test",
					TestValue: "some-testvalue-1",
				},
				{
					ID:        uuid.MustParse("eab8118c-45e9-4848-a380-ed6d981f2338"),
					Name:      "test",
					TestValue: "someaccvalue",
				},
			},
		},
		"complex query": {
			records: []*MockModel{
				{
					ID:        uuid.MustParse("885b50a8-f2d2-4fc2-b8e8-4db54f5ef5b6"),
					Name:      "test",
					TestValue: "prdvalue",
				},
				{
					ID:        uuid.MustParse("d8c9b566-f711-4113-8a86-a07fa470e43a"),
					Name:      "prd",
					TestValue: "accvalue",
				},
				{
					ID:        uuid.MustParse("87e8ed33-512d-4482-b639-e0830a19b653"),
					Name:      "test",
					TestValue: "prdvalue",
				},
				{
					ID:        uuid.MustParse("96954f52-f87c-4ec2-9af5-3e13642bdc83"),
					Name:      "test",
					TestValue: "some-testvalue-1",
				},
				{
					ID:        uuid.MustParse("eab8118c-45e9-4848-a380-ed6d981f2338"),
					Name:      "test",
					TestValue: "someaccvalue",
				},
			},
			queryString: "contains(concat(testValue,name),'prd') or concat(name,concat(' ',concat('length ',length(tolower(testValue))))) eq 'test length 12'",
			expectedSql: "SELECT * FROM `mock_models` WHERE test_value || name LIKE '%prd%' OR name || ' ' || 'length ' || LENGTH(LOWER(test_value)) = 'test length 12'",
			expectedResult: []MockModel{
				{
					ID:        uuid.MustParse("885b50a8-f2d2-4fc2-b8e8-4db54f5ef5b6"),
					Name:      "test",
					TestValue: "prdvalue",
				},
				{
					ID:        uuid.MustParse("d8c9b566-f711-4113-8a86-a07fa470e43a"),
					Name:      "prd",
					TestValue: "accvalue",
				},
				{
					ID:        uuid.MustParse("87e8ed33-512d-4482-b639-e0830a19b653"),
					Name:      "test",
					TestValue: "prdvalue",
				},
				{
					ID:        uuid.MustParse("eab8118c-45e9-4848-a380-ed6d981f2338"),
					Name:      "test",
					TestValue: "someaccvalue",
				},
			},
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			// Arrange
			db := gormtestutil.NewMemoryDatabase(t, gormtestutil.WithName(t.Name()))
			_ = db.AutoMigrate(&MockModel{}, &Metadata{})
			db.CreateInBatches(testData.records, len(testData.records))

			odataFilter := NewOdataQueryBuilder()

			// Act
			var dbQuery *gorm.DB
			var err error
			var result []MockModel
			sqlQuery := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
				dbQuery, err = odataFilter.BuildQuery(testData.queryString, tx)
				return dbQuery.Find(&MockModel{})
			})

			dbQuery, err = odataFilter.BuildQuery(testData.queryString, db)

			queryResult := dbQuery.Find(&result)
			fmt.Printf("result: %+v\n", result)
			fmt.Printf("queryResult: %+v", queryResult)

			// Assert
			assert.NoError(t, err)
			assert.NotNil(t, dbQuery)
			assert.Equal(t, testData.expectedSql, sqlQuery)
			assert.Equal(t, int(len(testData.expectedResult)), int(queryResult.RowsAffected))
			assert.Equal(t, testData.expectedResult, result)
		})
	}
}

func Test_BuildQuery_ErrorOnConstructTree(t *testing.T) {
	// Arrange
	db := gormtestutil.NewMemoryDatabase(t, gormtestutil.WithName(t.Name()))
	_ = db.AutoMigrate(&MockModel{}, &Metadata{})
	query := "length(name"

	odataFilter := NewOdataQueryBuilder()

	// Act
	_, err := odataFilter.BuildQuery(query, db)

	// Assert
	assert.Error(t, err)
}

func Test_BuildQuery_ErrorOnInvalidQuery(t *testing.T) {
	// Arrange
	db := gormtestutil.NewMemoryDatabase(t, gormtestutil.WithName(t.Name()))
	_ = db.AutoMigrate(&MockModel{}, &Metadata{})
	query := "length(name)"

	odataFilter := NewOdataQueryBuilder()

	// Act
	_, err := odataFilter.BuildQuery(query, db)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid query")
}

func Test_PrintTree_Success(t *testing.T) {
	// Arrange
	queryString := "name eq 'test' and testValue eq 'testvalue'"
	odataFilter := NewOdataQueryBuilder()

	// Act
	tree, err := odataFilter.PrintTree(queryString)

	// Assert
	assert.NoError(t, err)
	assert.NotEmpty(t, tree)
}

func Test_PrintTree_Error(t *testing.T) {
	// Arrange
	queryString := "name eq 'test' and (testValue eq 'testvalue' or testValue eq 'accvalue'"
	odataFilter := NewOdataQueryBuilder()

	// Act
	_, err := odataFilter.PrintTree(queryString)

	// Assert
	assert.Error(t, err)
}
