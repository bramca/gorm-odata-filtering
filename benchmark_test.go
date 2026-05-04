package gormodata

import (
	"regexp"
	"testing"

	syntaxtree "github.com/bramca/go-syntax-tree"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func Benchmark_WithoutValidation(b *testing.B) {
	query := "contains(concat(testValue,name),'prd') or concat(name,concat(' ',concat('length ',length(tolower(testValue))))) eq 'test length 12'"
	db, _ := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	for b.Loop() {
		_, _ = BuildQuery(query, db, SQLite)
	}
}

func Benchmark_WithSQLInjectionValidation(b *testing.B) {
	query := "contains(concat(testValue,name),'prd') or concat(name,concat(' ',concat('length ',length(tolower(testValue))))) eq 'test length 12'"
	db, _ := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	for b.Loop() {
		_, _ = BuildQuery(query, db, SQLite, WithBadPatternValidation(map[*regexp.Regexp][]syntaxtree.NodeType{
			operandBadPattern: {
				syntaxtree.LeftOperand,
				syntaxtree.RightOperand,
			},
		}))
	}
}

func Benchmark_WithAllValidations(b *testing.B) {
	query := "contains(concat(testValue,name),'prd') or concat(name,concat(' ',concat('length ',length(tolower(testValue))))) eq 'test length 12'"
	db, _ := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	for b.Loop() {
		_, _ = BuildQuery(query, db, SQLite,
			WithInputModelValidation(MockModel{}),
			WithMaxTreeDepth(5),
			WithMaxObjectExpansion(2),
			WithBadPatternValidation(map[*regexp.Regexp][]syntaxtree.NodeType{
				operandBadPattern: {
					syntaxtree.LeftOperand,
					syntaxtree.RightOperand,
				},
			}))
	}
}
