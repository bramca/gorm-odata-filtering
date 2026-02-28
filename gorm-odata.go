package gormodata

import (
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"

	syntaxtree "github.com/bramca/go-syntax-tree"
	"github.com/survivorbat/go-tsyncmap"

	deepgorm "github.com/survivorbat/gorm-deep-filtering"
	gormqonvert "github.com/survivorbat/gorm-query-convert"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type DbType int

const (
	PostgreSQL DbType = iota
	MySQL
	SQLite
	SQLServer
)

var (
	cacheGormqonvertTranslationMap = tsyncmap.Map[string, map[string]string]{}
	operatorTranslation            = map[string]string{
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

	operatorTranslationReversed = map[string]string{
		"eq":         "!=",
		"ne":         "=",
		"lt":         ">=",
		"le":         ">",
		"gt":         "<=",
		"ge":         "<",
		"contains":   "!~",
		"startswith": "!~",
		"endswith":   "!~",
	}

	gormqonvertTranslation = map[string]string{
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

	gormqonvertTranslationReversed = map[string]string{
		"eq":         "!=",
		"ne":         "=",
		"lt":         ">=",
		"le":         ">",
		"gt":         "<=",
		"ge":         "<",
		"contains":   "!~",
		"startswith": "!~",
		"endswith":   "!~",
	}

	unaryFunctionTranslation = map[DbType]map[string]string{
		PostgreSQL: {
			"length":           "LENGTH",
			"indexof":          "POSITION",
			"tolower":          "LOWER",
			"toupper":          "UPPER",
			"trim":             "TRIM",
			"year":             "EXTRACT(YEAR FROM %s)",
			"month":            "EXTRACT(MONTH FROM %s)",
			"day":              "EXTRACT(DAY FROM %s)",
			"hour":             "EXTRACT(HOUR FROM %s)",
			"minute":           "EXTRACT(MINUTE FROM %s)",
			"second":           "EXTRACT(SECOND FROM %s)",
			"fractionalsecond": "EXTRACT(MICROSECOND FROM %s)",
			"date":             "TO_DATE",
			"time":             "CAST(%s::timestamp AS time)",
			"now":              "NOW",
			"round":            "ROUND",
			"floor":            "FLOOR",
			"ceiling":          "CEIL",
		},
		MySQL: {
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
		},
		SQLite: {
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
		},
		SQLServer: {
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
		},
	}

	operatorPrecedence = []string{
		"not",
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
	operatorParsers = []syntaxtree.OperatorParser{
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

	binaryFunctionParsers = []syntaxtree.BinaryFunctionParser{
		{
			FunctionName:     "concat",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
			OperandSeparator: ',',
		},
		{
			FunctionName:     "contains",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
			OperandSeparator: ',',
		},
		{
			FunctionName:     "endswith",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
			OperandSeparator: ',',
		},
		{
			FunctionName:     "startswith",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
			OperandSeparator: ',',
		},
	}

	unaryFunctionParsers = []syntaxtree.UnaryFunctionParser{
		{
			FunctionName:     "not",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "length",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "indexof",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "tolower",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "toupper",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "trim",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "year",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "month",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "day",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "hour",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "minute",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "second",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "fractionalsecond",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "date",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "time",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "now",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "round",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "floor",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
		{
			FunctionName:     "ceiling",
			OpeningDelimiter: '(',
			ClosingDelimiter: ')',
		},
	}
)

// PrintTree
// Get a printable version of the abstract syntax tree for a given query
func PrintTree(query string) (string, error) {
	tree, err := GetAST(query)
	if err != nil {
		return "", err
	}

	return tree.String(), nil
}

// GetAST
// Get the full abstract syntaxtree for a given query
func GetAST(query string) (syntaxtree.SyntaxTree, error) {
	tree := syntaxtree.SyntaxTree{
		OperatorPrecedence:    operatorPrecedence,
		OperatorParsers:       operatorParsers,
		BinaryFunctionParsers: binaryFunctionParsers,
		UnaryFunctionParsers:  unaryFunctionParsers,
		Separator:             ";",
	}

	err := tree.ConstructTree(query)
	if err != nil {
		return syntaxtree.SyntaxTree{}, err
	}

	return tree, nil
}

// ValidQuery
// Validates input query against an input gorm model
//   - checks max tree depth if set > 0
//   - checks left operands for being existing column names
func ValidQuery(query string, input any, maxTreeDepth int, db *gorm.DB) error {
	columnNamesList := columnNames(input, db.NamingStrategy)
	tree := syntaxtree.SyntaxTree{
		OperatorPrecedence:    operatorPrecedence,
		OperatorParsers:       operatorParsers,
		BinaryFunctionParsers: binaryFunctionParsers,
		UnaryFunctionParsers:  unaryFunctionParsers,
		Separator:             ";",
	}

	err := tree.ConstructTree(query)
	if err != nil {
		return err
	}

	return validateQuery(tree, maxTreeDepth, columnNamesList, db.NamingStrategy)
}

// BuildQueryWithValidation
// Builds a gorm query based on an odata query string
// with extra pre-validation on the input query.
//
// It validates input query against an input gorm model
//   - checks max tree depth if set > 0
//   - checks left operands for being existing column names
func BuildQueryWithValidation(query string, db *gorm.DB, databaseType DbType, input any, maxTreeDepth int) (*gorm.DB, error) {
	var err error
	db, err = checkDbPlugins(db)
	if err != nil {
		return db, err
	}

	tree := syntaxtree.SyntaxTree{
		OperatorPrecedence:    operatorPrecedence,
		OperatorParsers:       operatorParsers,
		BinaryFunctionParsers: binaryFunctionParsers,
		UnaryFunctionParsers:  unaryFunctionParsers,
		Separator:             ";",
	}

	err = tree.ConstructTree(query)
	if err != nil {
		return db, err
	}

	columnNamesList := columnNames(input, db.NamingStrategy)

	err = validateQuery(tree, maxTreeDepth, columnNamesList, db.NamingStrategy)
	if err != nil {
		return db, err
	}

	columnTranslationFunc := func(s string) string {
		return db.NamingStrategy.ColumnName("", s)
	}

	db, err = buildGormQuery(tree.Root, db, databaseType, operatorTranslation, gormqonvertTranslation, columnTranslationFunc, false)

	return db, err
}

// BuildQuery
// Builds a gorm query based on an odata query string
// using the default database naming strategy for translating columns
//
// WARNING: this function does not validate the input query against the input gorm model.
//
// It is advised to use either the ValidQuery function before or the BuildQueryWithValidation instead
func BuildQuery(query string, db *gorm.DB, databaseType DbType) (*gorm.DB, error) {
	var err error
	db, err = checkDbPlugins(db)
	if err != nil {
		return db, err
	}

	tree := syntaxtree.SyntaxTree{
		OperatorPrecedence:    operatorPrecedence,
		OperatorParsers:       operatorParsers,
		BinaryFunctionParsers: binaryFunctionParsers,
		UnaryFunctionParsers:  unaryFunctionParsers,
		Separator:             ";",
	}

	err = tree.ConstructTree(query)
	if err != nil {
		return db, err
	}

	columnTranslationFunc := func(s string) string {
		return db.NamingStrategy.ColumnName("", s)
	}

	db, err = buildGormQuery(tree.Root, db, databaseType, operatorTranslation, gormqonvertTranslation, columnTranslationFunc, false)

	return db, err
}

func buildGormQuery(root *syntaxtree.Node, db *gorm.DB, databaseType DbType, opTranslation map[string]string, gqTranslation map[string]string, columnTranslation func(string) string, notEnabled bool) (*gorm.DB, error) {
	cleanDB := db.Session(&gorm.Session{NewDB: true})
	switch root.Type {
	case syntaxtree.Operator:
		switch root.Value {
		case "and":
			if notEnabled {
				db = db.Where(buildGormQuery(root.LeftChild, cleanDB, databaseType, opTranslation, gqTranslation, columnTranslation, notEnabled)).Or(buildGormQuery(root.RightChild, cleanDB, databaseType, opTranslation, gqTranslation, columnTranslation, notEnabled))
			} else {
				db = db.Where(buildGormQuery(root.LeftChild, cleanDB, databaseType, opTranslation, gqTranslation, columnTranslation, notEnabled)).Where(buildGormQuery(root.RightChild, cleanDB, databaseType, opTranslation, gqTranslation, columnTranslation, notEnabled))
			}
		case "or":
			if notEnabled {
				db = db.Where(buildGormQuery(root.LeftChild, cleanDB, databaseType, opTranslation, gqTranslation, columnTranslation, notEnabled)).Where(buildGormQuery(root.RightChild, cleanDB, databaseType, opTranslation, gqTranslation, columnTranslation, notEnabled))
			} else {
				db = db.Where(buildGormQuery(root.LeftChild, cleanDB, databaseType, opTranslation, gqTranslation, columnTranslation, notEnabled)).Or(buildGormQuery(root.RightChild, cleanDB, databaseType, opTranslation, gqTranslation, columnTranslation, notEnabled))
			}
		case "eq", "ne", "lt", "le", "gt", "ge":
			// Build up left child
			leftChild := root.LeftChild
			queryLeftOperandString := ""
			if leftChild.Type == syntaxtree.UnaryOperator {
				queryLeftOperandString = buildUnaryFuncChain(databaseType, columnTranslation, leftChild)
			}
			if leftChild.Value == "concat" {
				queryLeftOperandString = buildConcat(databaseType, columnTranslation, leftChild)
			}
			if leftChild.Type == syntaxtree.LeftOperand {
				queryLeftOperandString = columnTranslation(leftChild.Value)
			}

			// Build up right child
			rightChild := root.RightChild
			queryRightOperandString := ""
			if rightChild.Type == syntaxtree.UnaryOperator {
				return db, &InvalidQueryError{
					Msg: "unary operators not supported as right operand of equality operators",
				}
			}
			if rightChild.Value == "concat" {
				return db, &InvalidQueryError{
					Msg: "concat not supported as right operand of equality operators",
				}
			}
			if rightChild.Type == syntaxtree.RightOperand {
				queryRightOperandString = strings.ReplaceAll(rightChild.Value, "'", "")
			}

			// If the leftoperand contains an expansion token ('/') then it should create a map according to this format
			// Needs gorm-deep-filtering (https://github.com/survivorbat/gorm-deep-filtering) enabled and gorm-query-qonvert (https://github.com/survivorbat/gorm-query-convert)
			filterMap := map[string]any{}
			currentMap := filterMap
			if strings.Contains(leftChild.Value, "/") {
				queryRightOperandString = strings.ReplaceAll(queryRightOperandString, "'", "")
				fieldSplit := strings.Split(leftChild.Value, "/")
				for i, field := range fieldSplit {
					fieldSnakeCase := columnTranslation(field)
					if i < len(fieldSplit)-1 {
						currentMap[fieldSnakeCase] = map[string]any{}
						currentMap = currentMap[fieldSnakeCase].(map[string]any)
						continue
					}
					currentMap[fieldSnakeCase] = queryRightOperandString
					if root.Value != "eq" {
						currentMap[fieldSnakeCase] = gqTranslation[root.Value] + currentMap[fieldSnakeCase].(string)
					}
				}
				db = db.Where(filterMap)
			} else {
				queryString := fmt.Sprintf("%s %s ?", queryLeftOperandString, opTranslation[root.Value])
				if queryRightOperandInt, err := strconv.Atoi(queryRightOperandString); err == nil {
					db = db.Where(queryString, queryRightOperandInt)
				} else {
					db = db.Where(queryString, queryRightOperandString)
				}
			}
		case "contains", "startswith", "endswith":
			// Build up left child
			leftChild := root.LeftChild
			queryLeftOperandString := ""
			if leftChild.Type == syntaxtree.UnaryOperator {
				queryLeftOperandString = buildUnaryFuncChain(databaseType, columnTranslation, leftChild)
			}
			if leftChild.Value == "concat" {
				queryLeftOperandString = buildConcat(databaseType, columnTranslation, leftChild)
			}
			if leftChild.Type == syntaxtree.LeftOperand {
				queryLeftOperandString = columnTranslation(leftChild.Value)
			}

			// Build up right child
			queryRightOperandString := root.RightChild.Value
			escapeContains := false
			rightOperandTranslation := map[string]string{
				"contains":   `%$1%`,
				"startswith": `$1%`,
				"endswith":   `%$1`,
			}
			if strings.Contains(queryRightOperandString, "%") {
				queryRightOperandString = strings.ReplaceAll(queryRightOperandString, "%", "\\%")
				escapeContains = true
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
					fieldSnakeCase := columnTranslation(field)
					if i < len(fieldSplit)-1 {
						currentMap[fieldSnakeCase] = map[string]any{}
						currentMap = currentMap[fieldSnakeCase].(map[string]any)
						continue
					}
					currentMap[fieldSnakeCase] = gqTranslation[root.Value] + queryRightOperandString
				}
				db = db.Where(filterMap)
			} else {
				replacementString := "%s LIKE ?"
				if notEnabled {
					replacementString = "%s NOT LIKE ?"
				}

				if escapeContains {
					replacementString += " ESCAPE '\\'"
				}
				queryString := fmt.Sprintf(replacementString, queryLeftOperandString)
				db = db.Where(queryString, queryRightOperandString)
			}
		}
	case syntaxtree.UnaryOperator:
		if root.Value != "not" {
			return db, &InvalidQueryError{
				Msg: "root level operators other then 'not' are not supported",
			}
		}
		var err error
		db, err = buildGormQuery(root.LeftChild, db, databaseType, operatorTranslationReversed, gormqonvertTranslationReversed, columnTranslation, true)
		if err != nil {
			return db, err
		}
	default:
		return db, &InvalidQueryError{
			Msg: "unknown query type",
		}
	}

	return db, nil
}

func buildConcat(databaseType DbType, columnTranslation func(string) string, root *syntaxtree.Node) string {
	result := ""
	if root.Value == "concat" {
		result = fmt.Sprintf("%s || %s", buildConcat(databaseType, columnTranslation, root.LeftChild), buildConcat(databaseType, columnTranslation, root.RightChild))
	}
	if root.Type == syntaxtree.UnaryOperator {
		result = buildUnaryFuncChain(databaseType, columnTranslation, root)
	}

	if root.Type == syntaxtree.LeftOperand || root.Type == syntaxtree.RightOperand {
		result = root.Value
		if !strings.Contains(result, "'") {
			result = columnTranslation(result)
		}
	}

	return result
}

func buildUnaryFuncChain(databaseType DbType, columnTranslation func(string) string, root *syntaxtree.Node) string {
	result := ""
	nodesVisited := map[int]bool{}
	for !nodesVisited[root.Id] && root.Type == syntaxtree.UnaryOperator {
		if root.LeftChild != nil && root.LeftChild.Type == syntaxtree.UnaryOperator && !nodesVisited[root.LeftChild.Id] {
			root = root.LeftChild
			continue
		}
		nodesVisited[root.Id] = true
		if result == "" {
			if strings.Contains(unaryFunctionTranslation[databaseType][root.Value], "%") {
				result = fmt.Sprintf(unaryFunctionTranslation[databaseType][root.Value], columnTranslation(root.LeftChild.Value))
			} else {
				result = fmt.Sprintf("%s(%s)", unaryFunctionTranslation[databaseType][root.Value], columnTranslation(root.LeftChild.Value))
			}
		} else {
			result = fmt.Sprintf("%s(%s)", unaryFunctionTranslation[databaseType][root.Value], result)
		}

		if root.Parent != nil {
			root = root.Parent
		}
	}

	return result
}

func checkDbPlugins(db *gorm.DB) (*gorm.DB, error) {
	if _, ok := db.Plugins[deepgorm.New().Name()]; !ok {
		if err := db.Use(deepgorm.New()); err != nil {
			return db, err
		}
	}
	if _, ok := db.Plugins[gormqonvert.New(gormqonvert.CharacterConfig{}).Name()]; ok {
		plugin := db.Plugins[gormqonvert.New(gormqonvert.CharacterConfig{}).Name()]
		pluginConfig := reflect.ValueOf(plugin).Elem().FieldByName("config")
		if gormqonvertTranslationMap, cacheOk := cacheGormqonvertTranslationMap.Load("gormqonvertTranslation"); !cacheOk {
			gormqonvertTranslation["gt"] = pluginConfig.FieldByName("GreaterThanPrefix").String()
			gormqonvertTranslation["ge"] = pluginConfig.FieldByName("GreaterOrEqualToPrefix").String()
			gormqonvertTranslation["lt"] = pluginConfig.FieldByName("LessThanPrefix").String()
			gormqonvertTranslation["le"] = pluginConfig.FieldByName("LessOrEqualToPrefix").String()
			gormqonvertTranslation["ne"] = pluginConfig.FieldByName("NotEqualToPrefix").String()
			gormqonvertTranslation["contains"] = pluginConfig.FieldByName("LikePrefix").String()
			gormqonvertTranslation["startswith"] = pluginConfig.FieldByName("LikePrefix").String()
			gormqonvertTranslation["endswith"] = pluginConfig.FieldByName("LikePrefix").String()
		} else {
			gormqonvertTranslation = gormqonvertTranslationMap
		}
		if gormqonvertTranslationMap, cacheOk := cacheGormqonvertTranslationMap.Load("gormqonvertTranslationReversed"); !cacheOk {
			gormqonvertTranslationReversed["gt"] = pluginConfig.FieldByName("LessThanPrefix").String()
			gormqonvertTranslationReversed["ge"] = pluginConfig.FieldByName("LessOrEqualToPrefix").String()
			gormqonvertTranslationReversed["lt"] = pluginConfig.FieldByName("GreaterThanPrefix").String()
			gormqonvertTranslationReversed["le"] = pluginConfig.FieldByName("GreaterOrEqualToPrefix").String()
			gormqonvertTranslationReversed["ne"] = ""
			gormqonvertTranslationReversed["contains"] = pluginConfig.FieldByName("NotLikePrefix").String()
			gormqonvertTranslationReversed["startswith"] = pluginConfig.FieldByName("NotLikePrefix").String()
			gormqonvertTranslationReversed["endswith"] = pluginConfig.FieldByName("NotLikePrefix").String()
			cacheGormqonvertTranslationMap.Store("gormqonvertTranslationReversed", gormqonvertTranslationReversed)
		} else {
			gormqonvertTranslationReversed = gormqonvertTranslationMap
		}
	} else {
		config := gormqonvert.CharacterConfig{
			GreaterThanPrefix:      gormqonvertTranslation["gt"],
			GreaterOrEqualToPrefix: gormqonvertTranslation["ge"],
			LessThanPrefix:         gormqonvertTranslation["lt"],
			LessOrEqualToPrefix:    gormqonvertTranslation["le"],
			NotEqualToPrefix:       gormqonvertTranslation["ne"],
			LikePrefix:             gormqonvertTranslation["contains"],
			NotLikePrefix:          gormqonvertTranslationReversed["contains"],
		}
		if err := db.Use(gormqonvert.New(config)); err != nil {
			return db, err
		}
		cacheGormqonvertTranslationMap.Store("gormqonvertTranslation", gormqonvertTranslation)
		cacheGormqonvertTranslationMap.Store("gormqonvertTranslationReversed", gormqonvertTranslationReversed)
	}

	return db, nil
}

func validateQuery(tree syntaxtree.SyntaxTree, maxTreeDepth int, columnNamesList []string, schemaNamer schema.Namer) error {
	depth := 0
	currentNode := tree.Root
	nodesVisited := map[int]bool{}

	for !nodesVisited[currentNode.Id] {
		if maxTreeDepth > 0 && depth > maxTreeDepth {
			return &InvalidQueryError{
				Msg: fmt.Sprintf("maximum query complexity exceeded: %d > %d", depth, maxTreeDepth),
			}
		}
		if currentNode.Type == syntaxtree.Operator || currentNode.Type == syntaxtree.UnaryOperator {
			if currentNode.LeftChild != nil && !nodesVisited[currentNode.LeftChild.Id] {
				currentNode = currentNode.LeftChild
				depth += 1

				continue
			}
			if currentNode.RightChild != nil && !nodesVisited[currentNode.RightChild.Id] {
				currentNode = currentNode.RightChild
				depth += 1

				continue
			}

		}

		if currentNode.Type == syntaxtree.LeftOperand && currentNode.Parent.Value != "concat" {
			columnName := schemaNamer.ColumnName("", currentNode.Value)
			if strings.Contains(columnName, "/") {
				splitName := strings.Split(columnName, "/")
				columnName = splitName[0]
			}
			if !slices.Contains(columnNamesList, columnName) {
				return &InvalidQueryError{
					Msg: fmt.Sprintf("unknown column name '%s'", columnName),
				}
			}
		}

		nodesVisited[currentNode.Id] = true

		if currentNode.Parent != nil {
			currentNode = currentNode.Parent
			depth -= 1
		}
	}

	return nil
}

func tableName(input any, schemaNamer schema.Namer) string {
	tabler, ok := input.(schema.Tabler)
	if ok {
		return tabler.TableName()
	}

	typeOf := reflect.TypeOf(input)
	return schemaNamer.TableName(typeOf.Name())
}

func columnNames(input any, schemaNamer schema.Namer) []string {
	tableName := tableName(input, schemaNamer)
	typeOf := reflect.TypeOf(input)
	flds := typeOf.NumField()
	res := make([]string, flds)
	for i := range flds {
		fld := typeOf.Field(i)
		name := fld.Name

		var gormName string
		if tag := fld.Tag.Get("gorm"); tag != "" {
			for setting := range strings.SplitSeq(tag, ";") {
				if !strings.HasPrefix(setting, "column:") {
					continue
				}

				gormName = strings.TrimPrefix(setting, "column:")
			}
		}

		if gormName == "" {
			gormName = schemaNamer.ColumnName(tableName, name)
		}

		res[i] = gormName
	}

	return res
}
