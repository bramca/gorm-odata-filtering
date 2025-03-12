package gormodata

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ing-bank/gormtestutil"
	"github.com/survivorbat/ptr"
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
	ID    uuid.UUID
	Name  string
	Tag   *Tag `gorm:"foreignKey:TagID"`
	TagID *uuid.UUID
}

type Tag struct {
	ID    uuid.UUID
	Value string
}

type MockTimeModel struct {
	Name      string
	CreatedAt time.Time
}

func Test_BuildQuery_CorrectQueryForDbType(t *testing.T) {
	tests := map[string]struct {
		queryString string
		expectedSql string
		dbType      DbType
	}{
		"PostgreSQL": {
			queryString: "year(createdAt) gt 2025 and time(createdAt) lt '01:12:00'",
			expectedSql: "SELECT * FROM `mock_time_models` WHERE EXTRACT(YEAR FROM created_at) > 2025 AND CAST(created_at::timestamp AS time) < '01:12:00'",
			dbType:      PostgreSQL,
		},
		"MySQL": {
			queryString: "year(createdAt) gt 2025 and time(createdAt) lt '01:12:00'",
			expectedSql: "SELECT * FROM `mock_time_models` WHERE YEAR(created_at) > 2025 AND TIME(created_at) < '01:12:00'",
			dbType:      MySQL,
		},
		"SQLServer": {
			queryString: "year(createdAt) gt 2025 and time(createdAt) lt '01:12:00'",
			expectedSql: "SELECT * FROM `mock_time_models` WHERE YEAR(created_at) > 2025 AND TIME(created_at) < '01:12:00'",
			dbType:      SQLServer,
		},
		"SQLite": {
			queryString: "year(createdAt) gt 2025 and time(createdAt) lt '01:12:00'",
			expectedSql: "SELECT * FROM `mock_time_models` WHERE YEAR(created_at) > 2025 AND TIME(created_at) < '01:12:00'",
			dbType:      SQLite,
		},
	}
	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			// Arrange
			db := gormtestutil.NewMemoryDatabase(t, gormtestutil.WithName(t.Name()))
			_ = db.AutoMigrate(&MockTimeModel{})

			// Act
			var dbQuery *gorm.DB
			var err error
			sqlQuery := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
				dbQuery, err = BuildQuery(testData.queryString, tx, testData.dbType)
				return dbQuery.Find(&MockTimeModel{})
			})

			// Assert
			assert.NoError(t, err)
			assert.NotNil(t, dbQuery)
			assert.Equal(t, testData.expectedSql, sqlQuery)
		})
	}
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

			// Act
			var dbQuery *gorm.DB
			var err error
			var result []MockModel
			sqlQuery := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
				dbQuery, err = BuildQuery(testData.queryString, tx, SQLite)
				return dbQuery.Find(&MockModel{})
			})

			dbQuery, err = BuildQuery(testData.queryString, db, SQLite)

			queryResult := dbQuery.Find(&result)

			// Assert
			assert.NoError(t, err)
			assert.NotNil(t, dbQuery)
			assert.Equal(t, testData.expectedSql, sqlQuery)
			assert.Equal(t, int(len(testData.expectedResult)), int(queryResult.RowsAffected))
			assert.Equal(t, testData.expectedResult, result)
		})
	}
}

func Test_BuildQuery_ObjectExpansion(t *testing.T) {
	// Arrange
	mockModelRecords := []*MockModel{
		{
			ID:         uuid.MustParse("885b50a8-f2d2-4fc2-b8e8-4db54f5ef5b6"),
			Name:       "test",
			TestValue:  "prdvalue",
			MetadataID: ptr.Ptr(uuid.MustParse("1ea3cf2f-5c1f-47c6-b0c3-78f0cee2007b")),
			Metadata: &Metadata{
				ID:   uuid.MustParse("1ea3cf2f-5c1f-47c6-b0c3-78f0cee2007b"),
				Name: "test-1-metadata",
				Tag: &Tag{
					ID:    uuid.MustParse("93e75a82-1120-4a21-9995-b057c6b7a517"),
					Value: "test-1-value",
				},
			},
		},
		{
			ID:         uuid.MustParse("d8c9b566-f711-4113-8a86-a07fa470e43a"),
			Name:       "prd",
			TestValue:  "accvalue",
			MetadataID: ptr.Ptr(uuid.MustParse("6afa4aef-a646-415b-ae2d-1ab7fc554c08")),
			Metadata: &Metadata{
				ID:   uuid.MustParse("6afa4aef-a646-415b-ae2d-1ab7fc554c08"),
				Name: "prd-1-metadata",
				Tag: &Tag{
					ID:    uuid.MustParse("8dc750d5-9121-4269-be18-fe8f7b7fffb7"),
					Value: "prd-1-value",
				},
			},
		},
		{
			ID:         uuid.MustParse("87e8ed33-512d-4482-b639-e0830a19b653"),
			Name:       "test",
			TestValue:  "prdvalue",
			MetadataID: ptr.Ptr(uuid.MustParse("200c2712-cafc-4f00-b6e1-0ff89871f1cd")),
			Metadata: &Metadata{
				ID:   uuid.MustParse("200c2712-cafc-4f00-b6e1-0ff89871f1cd"),
				Name: "test-2-metadata",
				Tag: &Tag{
					ID:    uuid.MustParse("605f54df-7983-470e-bc27-41dd9c7c14d8"),
					Value: "test-2-value",
				},
			},
		},
		{
			ID:         uuid.MustParse("96954f52-f87c-4ec2-9af5-3e13642bdc83"),
			Name:       "test",
			TestValue:  "some-testvalue-1",
			MetadataID: ptr.Ptr(uuid.MustParse("93ce3788-9e09-462a-a219-12373675d7e8")),
			Metadata: &Metadata{
				ID:   uuid.MustParse("93ce3788-9e09-462a-a219-12373675d7e8"),
				Name: "test-3-metadata",
				Tag: &Tag{
					ID:    uuid.MustParse("911bd72a-09f3-425f-942b-1df1cf0220e6"),
					Value: "test-3-value",
				},
			},
		},
		{
			ID:         uuid.MustParse("eab8118c-45e9-4848-a380-ed6d981f2338"),
			Name:       "test",
			TestValue:  "someaccvalue",
			MetadataID: ptr.Ptr(uuid.MustParse("d96c6f36-9dc9-4a07-a83b-11b62d8ff7db")),
			Metadata: &Metadata{
				ID:   uuid.MustParse("d96c6f36-9dc9-4a07-a83b-11b62d8ff7db"),
				Name: "test-4-metadata",
				Tag: &Tag{
					ID:    uuid.MustParse("83fc9b56-9e32-4a1a-876d-70d4605753c7"),
					Value: "test-4-value",
				},
			},
		},
	}
	expectedResult := []MockModel{
		{
			ID:         uuid.MustParse("96954f52-f87c-4ec2-9af5-3e13642bdc83"),
			Name:       "test",
			TestValue:  "some-testvalue-1",
			MetadataID: ptr.Ptr(uuid.MustParse("93ce3788-9e09-462a-a219-12373675d7e8")),
		},
		{
			ID:         uuid.MustParse("eab8118c-45e9-4848-a380-ed6d981f2338"),
			Name:       "test",
			TestValue:  "someaccvalue",
			MetadataID: ptr.Ptr(uuid.MustParse("d96c6f36-9dc9-4a07-a83b-11b62d8ff7db")),
		},
	}
	db := gormtestutil.NewMemoryDatabase(t, gormtestutil.WithName(t.Name()))
	_ = db.AutoMigrate(&MockModel{}, &Metadata{})
	db.CreateInBatches(mockModelRecords, len(mockModelRecords))
	expectedSql := "SELECT * FROM `mock_models` WHERE name = 'test' AND (metadata_id IN (SELECT `id` FROM `metadata` WHERE `metadata`.`name` = \"test-4-metadata\") OR metadata_id IN (SELECT `id` FROM `metadata` WHERE tag_id IN (SELECT `id` FROM `tags` WHERE tags.value LIKE \"test-3%\")))"

	queryString := "name eq 'test' and (metadata/name eq 'test-4-metadata' or startswith(metadata/tag/value,'test-3'))"

	// Act
	var dbQuery *gorm.DB
	var err error
	var result []MockModel
	sqlQuery := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		dbQuery, err = BuildQuery(queryString, tx, SQLite)
		return dbQuery.Find(&MockModel{})
	})
	dbQuery, err = BuildQuery(queryString, db, SQLite)
	queryResult := dbQuery.Find(&result)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, dbQuery)
	assert.Equal(t, int(len(expectedResult)), int(queryResult.RowsAffected))
	assert.Equal(t, expectedSql, sqlQuery)
	assert.Equal(t, expectedResult, result)
}

func Test_BuildQuery_ErrorOnConstructTree(t *testing.T) {
	// Arrange
	db := gormtestutil.NewMemoryDatabase(t, gormtestutil.WithName(t.Name()))
	_ = db.AutoMigrate(&MockModel{}, &Metadata{})
	query := "length(name"

	// Act
	_, err := BuildQuery(query, db, SQLite)

	// Assert
	assert.Error(t, err)
}

func Test_BuildQuery_ErrorOnInvalidQuery(t *testing.T) {
	// Arrange
	db := gormtestutil.NewMemoryDatabase(t, gormtestutil.WithName(t.Name()))
	_ = db.AutoMigrate(&MockModel{}, &Metadata{})
	query := "length(name)"

	// Act
	_, err := BuildQuery(query, db, SQLite)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid query")
}

func Test_PrintTree_Success(t *testing.T) {
	// Arrange
	queryString := "name eq 'test' and testValue eq 'testvalue'"

	// Act
	tree, err := PrintTree(queryString)

	// Assert
	assert.NoError(t, err)
	assert.NotEmpty(t, tree)
}

func Test_PrintTree_Error(t *testing.T) {
	// Arrange
	queryString := "name eq 'test' and (testValue eq 'testvalue' or testValue eq 'accvalue'"

	// Act
	_, err := PrintTree(queryString)

	// Assert
	assert.Error(t, err)
}
