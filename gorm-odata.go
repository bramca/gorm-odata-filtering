package gormodata

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	syntaxtree "github.com/bramca/go-syntax-tree"
	"github.com/stoewer/go-strcase"
	deepgorm "github.com/survivorbat/gorm-deep-filtering"
	gormqonvert "github.com/survivorbat/gorm-query-convert"
	"gorm.io/gorm"
)

var (
	operatorTranslation = map[string]string{
		"eq":         "=",
		"ne":         "!=",
		"lt":         "<",
		"le":         "<=",
		"gt":         ">",
		"ge":         ">=",
		"contains":   "~",
		"startswith": "~",
		"endswith":   "~",
	}

	unaryFunctionTranslation = map[string]string{
		"length":           "LENGTH",
		"indexof":          "LOCATE",
		"tolower":          "LOWER",
		"toupper":          "UPPER",
		"trim":             "TRIM",
		"year":             "YEAR",
		"month":            "MONTH",
		"day":              "DAY",
		"hour":             "HOUR",
		"minute":           "MINUTE",
		"second":           "SECOND",
		"fractionalsecond": "MICROSECOND",
		"date":             "DATE",
		"time":             "TIME",
		"now":              "NOW",
		"round":            "ROUND",
		"floor":            "FLOOR",
		"ceiling":          "CEIL",
	}
)

type OdataQueryBuilder struct {
	OperatorPrecedence []string
	OperatorParsers    []syntaxtree.OperatorParser
	BinaryFunctions    []syntaxtree.BinaryFunctionParser
	UnaryFunctions     []syntaxtree.UnaryFunctionParser
}

func NewOdataQueryBuilder() *OdataQueryBuilder {
	o := &OdataQueryBuilder{}
	o.OperatorPrecedence = []string{
		"length",
		"indexof",
		"tolower",
		"toupper",
		"trim",
		"year",
		"month",
		"day",
		"hour",
		"minute",
		"second",
		"fractionalsecond",
		"date",
		"time",
		"now",
		"round",
		"floor",
		"ceiling",
		"concat",
		"contains",
		"endswith",
		"startswith",
		"eq",
		"ne",
		"gt",
		"ge",
		"lt",
		"le",
		"and",
		"or",
	}

	o.OperatorParsers = []syntaxtree.OperatorParser{
		{
			OperatorString:  "eq",
			OperatorPattern: regexp.MustCompile(`(.*?) eq (.*?)`),
		},
		{
			OperatorString:  "ne",
			OperatorPattern: regexp.MustCompile(`(.*?) ne (.*?)`),
		},
		{
			OperatorString:  "gt",
			OperatorPattern: regexp.MustCompile(`(.*?) gt (.*?)`),
		},
		{
			OperatorString:  "ge",
			OperatorPattern: regexp.MustCompile(`(.*?) ge (.*?)`),
		},
		{
			OperatorString:  "lt",
			OperatorPattern: regexp.MustCompile(`(.*?) lt (.*?)`),
		},
		{
			OperatorString:  "le",
			OperatorPattern: regexp.MustCompile(`(.*?) le (.*?)`),
		},
		{
			OperatorString:  "and",
			OperatorPattern: regexp.MustCompile(`(.*?) and (.*?)`),
		},
		{
			OperatorString:  "or",
			OperatorPattern: regexp.MustCompile(`(.*?) or (.*?)`),
		},
	}

	binaryFunctions := []string{
		"concat",
		"contains",
		"endswith",
		"startswith",
	}

	binaryFunctionParsers := make([]syntaxtree.BinaryFunctionParser, len(binaryFunctions))
	for i, binaryFunction := range binaryFunctions {
		binaryFunctionParsers[i] = syntaxtree.BinaryFunctionParser{
			FunctionName:     binaryFunction,
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
			OperandSeparator: ',',
		}
	}
	o.BinaryFunctions = binaryFunctionParsers

	unaryFunctions := []string{
		"length",
		"indexof",
		"tolower",
		"toupper",
		"trim",
		"year",
		"month",
		"day",
		"hour",
		"minute",
		"second",
		"fractionalsecond",
		"date",
		"time",
		"now",
		"round",
		"floor",
		"ceiling",
	}

	unaryFunctionParsers := make([]syntaxtree.UnaryFunctionParser, len(unaryFunctions))
	for i, unaryFunction := range unaryFunctions {
		unaryFunctionParsers[i] = syntaxtree.UnaryFunctionParser{
			FunctionName:     unaryFunction,
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		}
	}

	o.UnaryFunctions = unaryFunctionParsers

	return o
}

func (o *OdataQueryBuilder) PrintTree(query string) (string, error) {
	tree := syntaxtree.SyntaxTree{
		OperatorPrecedence:    o.OperatorPrecedence,
		OperatorParsers:       o.OperatorParsers,
		BinaryFunctionParsers: o.BinaryFunctions,
		UnaryFunctionParsers:  o.UnaryFunctions,
		Separator:             ";",
	}

	err := tree.ConstructTree(query)
	if err != nil {
		return "", err
	}

	return tree.String(), nil
}

func (o *OdataQueryBuilder) BuildQuery(query string, db *gorm.DB) (*gorm.DB, error) {
	if err := db.Use(deepgorm.New()); err != nil && err != gorm.ErrRegistered {
		return db, err
	}
	config := gormqonvert.CharacterConfig{
		GreaterThanPrefix:      ">",
		GreaterOrEqualToPrefix: ">=",
		LessThanPrefix:         "<",
		LessOrEqualToPrefix:    "<=",
		NotEqualToPrefix:       "!=",
		LikePrefix:             "~",
		NotLikePrefix:          "!~",
	}
	if err := db.Use(gormqonvert.New(config)); err != nil && err != gorm.ErrRegistered {
		return db, err
	}
	tree := syntaxtree.SyntaxTree{
		OperatorPrecedence:    o.OperatorPrecedence,
		OperatorParsers:       o.OperatorParsers,
		BinaryFunctionParsers: o.BinaryFunctions,
		UnaryFunctionParsers:  o.UnaryFunctions,
		Separator:             ";",
	}

	err := tree.ConstructTree(query)
	if err != nil {
		return db, err
	}

	db, err = buildGormQuery(tree.Root, db)

	return db, err
}

func buildGormQuery(root *syntaxtree.Node, db *gorm.DB) (*gorm.DB, error) {
	switch root.Type {
	case syntaxtree.Operator:
		switch root.Value {
		case "and":
			db = db.Where(buildGormQuery(root.LeftChild, db)).Where(buildGormQuery(root.RightChild, db))
		case "or":
			db = db.Where(buildGormQuery(root.LeftChild, db)).Or(buildGormQuery(root.RightChild, db))
		case "eq", "ne", "lt", "le", "gt", "ge":
			// Build up left child
			leftChild := root.LeftChild
			queryLeftOperandString := ""
			if leftChild.Type == syntaxtree.UnaryOperator {
				queryLeftOperandString = buildUnaryFuncChain(leftChild)
			}
			if leftChild.Value == "concat" {
				queryLeftOperandString = buildConcat(leftChild)
			}
			if leftChild.Type == syntaxtree.LeftOperand {
				queryLeftOperandString = strcase.SnakeCase(leftChild.Value)
			}

			// Build up right child
			rightChild := root.RightChild
			queryRightOperandString := ""
			if rightChild.Type == syntaxtree.UnaryOperator {
				queryRightOperandString = buildUnaryFuncChain(rightChild)
			}
			if rightChild.Value == "concat" {
				queryRightOperandString = buildConcat(rightChild)
			}
			if rightChild.Type == syntaxtree.RightOperand {
				queryRightOperandString = rightChild.Value
			}

			// If the leftoperand contains an expansion token ('/') then it should create a map according to this format
			// Needs gorm-deep-filtering (https://github.com/survivorbat/gorm-deep-filtering) enabled and gorm-query-qonvert (https://github.com/survivorbat/gorm-query-convert)
			filterMap := map[string]any{}
			currentMap := filterMap
			if strings.Contains(leftChild.Value, "/") {
				queryRightOperandString = strings.ReplaceAll(queryRightOperandString, "'", "")
				fieldSplit := strings.Split(leftChild.Value, "/")
				for i, field := range fieldSplit {
					fieldSnakeCase := strcase.SnakeCase(field)
					if i < len(fieldSplit)-1 {
						currentMap[fieldSnakeCase] = map[string]any{}
						currentMap = currentMap[fieldSnakeCase].(map[string]any)
						continue
					}
					currentMap[fieldSnakeCase] = queryRightOperandString
					if root.Value != "eq" {
						currentMap[fieldSnakeCase] = operatorTranslation[root.Value] + currentMap[fieldSnakeCase].(string)
					}
				}
				db = db.Where(filterMap)
			} else {
				queryString := fmt.Sprintf("%s %s %s", queryLeftOperandString, operatorTranslation[root.Value], queryRightOperandString)
				db = db.Where(queryString)
			}
		case "contains", "startswith", "endswith":
			// Build up left child
			leftChild := root.LeftChild
			queryLeftOperandString := ""
			if leftChild.Type == syntaxtree.UnaryOperator {
				queryLeftOperandString = buildUnaryFuncChain(leftChild)
			}
			if leftChild.Value == "concat" {
				queryLeftOperandString = buildConcat(leftChild)
			}
			if leftChild.Type == syntaxtree.LeftOperand {
				queryLeftOperandString = strcase.SnakeCase(leftChild.Value)
			}

			// Build up right child
			queryRightOperandString := root.RightChild.Value
			rightOperandTranslation := map[string]string{
				"contains":   `'%$1%'`,
				"startswith": `'$1%'`,
				"endswith":   `'%$1'`,
			}

			queryRightOperandString = regexp.MustCompile(`'(.*)'`).ReplaceAllString(queryRightOperandString, rightOperandTranslation[root.Value])

			// If the leftoperand contains an expansion token ('/') then it should create a map according to this format
			// Needs gorm-deep-filtering (https://github.com/survivorbat/gorm-deep-filtering) enabled and gorm-query-qonvert (https://github.com/survivorbat/gorm-query-convert)
			filterMap := map[string]any{}
			currentMap := filterMap
			if strings.Contains(leftChild.Value, "/") {
				queryRightOperandString = strings.ReplaceAll(queryRightOperandString, "'", "")
				fieldSplit := strings.Split(leftChild.Value, "/")
				for i, field := range fieldSplit {
					fieldSnakeCase := strcase.SnakeCase(field)
					if i < len(fieldSplit)-1 {
						currentMap[fieldSnakeCase] = map[string]any{}
						currentMap = currentMap[fieldSnakeCase].(map[string]any)
						continue
					}
					currentMap[fieldSnakeCase] = operatorTranslation[root.Value] + queryRightOperandString
				}
				db = db.Where(filterMap)
			} else {
				queryString := fmt.Sprintf("%s LIKE %s", queryLeftOperandString, queryRightOperandString)
				db = db.Where(queryString)
			}
		}
	default:
		return db, errors.New("invalid query")
	}

	return db, nil
}

func buildConcat(root *syntaxtree.Node) string {
	result := ""
	if root.Value == "concat" {
		result = fmt.Sprintf("%s || %s", buildConcat(root.LeftChild), buildConcat(root.RightChild))
	}
	if root.Type == syntaxtree.UnaryOperator {
		result = buildUnaryFuncChain(root)
	}

	if root.Type == syntaxtree.LeftOperand {
		result = root.Value
		if !strings.Contains(result, "'") {
			result = strcase.SnakeCase(result)
		}
	}

	return result
}

func buildUnaryFuncChain(root *syntaxtree.Node) string {
	result := ""
	nodesVisited := map[int]bool{}
	for !nodesVisited[root.Id] && root.Type == syntaxtree.UnaryOperator {
		if root.LeftChild != nil && root.LeftChild.Type == syntaxtree.UnaryOperator && !nodesVisited[root.LeftChild.Id] {
			root = root.LeftChild
			continue
		}
		nodesVisited[root.Id] = true
		if result == "" {
			result = fmt.Sprintf("%s(%s)", unaryFunctionTranslation[root.Value], strcase.SnakeCase(root.LeftChild.Value))
		} else {
			result = fmt.Sprintf("%s(%s)", unaryFunctionTranslation[root.Value], result)
		}

		if root.Parent != nil {
			root = root.Parent
		}
	}

	return result
}
