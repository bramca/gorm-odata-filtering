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

	odataLexer = &syntaxtree.Lexer{
		BinaryOperators: []string{
			"eq",
			"ne",
			"gt",
			"ge",
			"lt",
			"le",
			"and",
			"or",
		},
		BinaryFunctions: []string{
			"concat",
			"contains",
			"endswith",
			"startswith",
		},
		UnaryFunctions: []string{
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
		},
		OpenDelimiter:             '(',
		CloseDelimiter:            ')',
		BinaryFunctionOpSeparator: ',',
		StringDelimiter:           '\'',
		TokenSeparator:            ' ',
	}

	odataPrecedence = map[string]int{
		"or":  1,
		"and": 2,
		"eq":  3,
		"ne":  3,
		"gt":  3,
		"ge":  3,
		"lt":  3,
		"le":  3,
	}
)

// QueryValidation
// is a type that can be used in the BuildQuery function to do some
//
// validations before building the gorm query
type QueryValidation func(tree *syntaxtree.SyntaxTree, db *gorm.DB) error

// PrintTree
// to get a printable version of the abstract syntax tree for a given query
func PrintTree(query string) (string, error) {
	tree, err := GetAST(query)
	if err != nil {
		return "", err
	}

	return tree.String(), nil
}

// GetAST
// to get the full abstract syntaxtree for a given query
func GetAST(query string) (*syntaxtree.SyntaxTree, error) {
	tree := &syntaxtree.SyntaxTree{
		Lexer:       odataLexer,
		Precendence: odataPrecedence,
	}

	err := tree.BuildTree(query)
	if err != nil {
		return nil, err
	}

	return tree, nil
}

// WithInputModelValidation
// returns a QueryValidation function that validates the input query against the input gorm model that needs to be filtered
func WithInputModelValidation(input any) QueryValidation {
	return func(tree *syntaxtree.SyntaxTree, db *gorm.DB) error {
		columnNamesList := columnNames(input, db.NamingStrategy)

		validationCheck := func(depth int, currentNode *syntaxtree.Node) error {
			if currentNode.Type == syntaxtree.LeftOperand && currentNode.Parent.Value != "concat" {
				columnName := db.NamingStrategy.ColumnName("", currentNode.Value)
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

			return nil
		}

		return validateQueryDepthFirstSearch(tree, validationCheck)
	}
}

// WithMaxTreeDepth
// returns a QueryValidation function that checks maximum syntax tree depth of the parsed query
func WithMaxTreeDepth(maxTreeDepth int) QueryValidation {
	return func(tree *syntaxtree.SyntaxTree, db *gorm.DB) error {
		validationCheck := func(depth int, currentNode *syntaxtree.Node) error {
			if depth > maxTreeDepth {
				return &InvalidQueryError{
					Msg: fmt.Sprintf("maximum query complexity exceeded: >%d", maxTreeDepth),
				}
			}

			return nil
		}

		return validateQueryDepthFirstSearch(tree, validationCheck)
	}
}

// WithMaxObjectExpansion
// returns a QueryValidation function that checks queries with object expansion (e.g. model/prop/value/...)
//
// for maximum object expansion depth
func WithMaxObjectExpansion(maxObjectExpansion int) QueryValidation {
	return func(tree *syntaxtree.SyntaxTree, db *gorm.DB) error {
		validationCheck := func(depth int, currentNode *syntaxtree.Node) error {
			if strings.Contains(currentNode.Value, "/") {
				splitName := strings.Split(currentNode.Value, "/")
				if len(splitName) > maxObjectExpansion {
					return &InvalidQueryError{
						Msg: fmt.Sprintf("query contains value '%s' that exceeds the maximum allowed object expansion depth: >%d", currentNode.Value, maxObjectExpansion),
					}
				}
			}

			return nil
		}

		return validateQueryDepthFirstSearch(tree, validationCheck)
	}
}

// BuildQuery
// builds a gorm query based on an odata query string
//
// You can add optional query validations from this package (see WithInputModelValidation, WithMaxObjectExpansion...)
//
// Or add your custom validation functions -> type QueryValidtion
func BuildQuery(query string, db *gorm.DB, databaseType DbType, queryValidations ...QueryValidation) (*gorm.DB, error) {
	var err error
	db, err = checkDbPlugins(db)
	if err != nil {
		return db, err
	}

	tree, err := GetAST(query)
	if err != nil {
		return db, err
	}

	for _, validateQuery := range queryValidations {
		if err := validateQuery(tree, db); err != nil {
			return db, err
		}
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

			queryRightOperandString = regexp.MustCompile(`\s*'(.*)'\s*`).ReplaceAllString(queryRightOperandString, rightOperandTranslation[root.Value])

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

func validateQueryDepthFirstSearch(tree *syntaxtree.SyntaxTree, validationChecks ...func(depth int, currentNode *syntaxtree.Node) error) error {
	depth := 0
	currentNode := tree.Root
	nodesVisited := map[int]bool{}

	for !nodesVisited[currentNode.Id] {
		for _, validationCheck := range validationChecks {
			if err := validationCheck(depth, currentNode); err != nil {
				return err
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
