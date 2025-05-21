package gormodata

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	syntaxtree "github.com/bramca/go-syntax-tree"
	"github.com/stoewer/go-strcase"
	"github.com/survivorbat/go-tsyncmap"
	deepgorm "github.com/survivorbat/gorm-deep-filtering"
	gormqonvert "github.com/survivorbat/gorm-query-convert"
	"gorm.io/gorm"
)

type DbType int

const (
	PostgreSQL DbType = iota
	MySQL
	SQLite
	SQLServer
)

var (
	unaryFunctionRegex             = regexp.MustCompile(`(.*)\((.*?)\)`)
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

func PrintTree(query string) (string, error) {
	tree := syntaxtree.SyntaxTree{
		OperatorPrecedence:    operatorPrecedence,
		OperatorParsers:       operatorParsers,
		BinaryFunctionParsers: binaryFunctionParsers,
		UnaryFunctionParsers:  unaryFunctionParsers,
		Separator:             ";",
	}

	err := tree.ConstructTree(query)
	if err != nil {
		return "", err
	}

	return tree.String(), nil
}

func BuildQuery(query string, db *gorm.DB, databaseType DbType) (*gorm.DB, error) {
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
	tree := syntaxtree.SyntaxTree{
		OperatorPrecedence:    operatorPrecedence,
		OperatorParsers:       operatorParsers,
		BinaryFunctionParsers: binaryFunctionParsers,
		UnaryFunctionParsers:  unaryFunctionParsers,
		Separator:             ";",
	}

	err := tree.ConstructTree(query)
	if err != nil {
		return db, err
	}

	db, err = buildGormQuery(tree.Root, db, databaseType, operatorTranslation, gormqonvertTranslation, false)

	return db, err
}

func buildGormQuery(root *syntaxtree.Node, db *gorm.DB, databaseType DbType, opTranslation map[string]string, gqTranslation map[string]string, notEnabled bool) (*gorm.DB, error) {
	cleanDB := db.Session(&gorm.Session{NewDB: true})
	switch root.Type {
	case syntaxtree.Operator:
		switch root.Value {
		case "and":
			if notEnabled {
				db = db.Where(buildGormQuery(root.LeftChild, cleanDB, databaseType, opTranslation, gqTranslation, notEnabled)).Or(buildGormQuery(root.RightChild, cleanDB, databaseType, opTranslation, gqTranslation, notEnabled))
			} else {
				db = db.Where(buildGormQuery(root.LeftChild, cleanDB, databaseType, opTranslation, gqTranslation, notEnabled)).Where(buildGormQuery(root.RightChild, cleanDB, databaseType, opTranslation, gqTranslation, notEnabled))
			}
		case "or":
			if notEnabled {
				db = db.Where(buildGormQuery(root.LeftChild, cleanDB, databaseType, opTranslation, gqTranslation, notEnabled)).Where(buildGormQuery(root.RightChild, cleanDB, databaseType, opTranslation, gqTranslation, notEnabled))
			} else {
				db = db.Where(buildGormQuery(root.LeftChild, cleanDB, databaseType, opTranslation, gqTranslation, notEnabled)).Or(buildGormQuery(root.RightChild, cleanDB, databaseType, opTranslation, gqTranslation, notEnabled))
			}
		case "eq", "ne", "lt", "le", "gt", "ge":
			// Build up left child
			leftChild := root.LeftChild
			queryLeftOperandString := ""
			if leftChild.Type == syntaxtree.UnaryOperator {
				queryLeftOperandString = buildUnaryFuncChain(databaseType, leftChild)
			}
			if leftChild.Value == "concat" {
				queryLeftOperandString = buildConcat(databaseType, leftChild)
			}
			if leftChild.Type == syntaxtree.LeftOperand {
				queryLeftOperandString = strcase.SnakeCase(leftChild.Value)
			}

			// Build up right child
			rightChild := root.RightChild
			queryRightOperandString := ""
			if rightChild.Type == syntaxtree.UnaryOperator {
				queryRightOperandString = buildUnaryFuncChain(databaseType, rightChild)
			}
			if rightChild.Value == "concat" {
				queryRightOperandString = buildConcat(databaseType, rightChild)
			}
			if rightChild.Type == syntaxtree.RightOperand {
				queryRightOperandString = rightChild.Value
			}

			// If the leftoperand contains an expansion token ('/') then it should create a map according to this format
			// Needs gorm-deep-filtering (https://github.com/survivorbat/gorm-deep-filtering) enabled and gorm-query-qonvert (https://github.com/survivorbat/gorm-query-convert)
			fmt.Printf("[DEBUG] queryLeftOperandString: %s\n", queryLeftOperandString)
			if strings.Contains(queryLeftOperandString, "/") {
				filterMap := buildNestedFilter(queryLeftOperandString, leftChild, root, gqTranslation)
				db = db.Where(filterMap)
			} else {
				queryString := fmt.Sprintf("%s %s %s", queryLeftOperandString, opTranslation[root.Value], queryRightOperandString)
				db = db.Where(queryString)
			}
		case "contains", "startswith", "endswith":
			// Build up left child
			leftChild := root.LeftChild
			queryLeftOperandString := ""
			if leftChild.Type == syntaxtree.UnaryOperator {
				queryLeftOperandString = buildUnaryFuncChain(databaseType, leftChild)
			}
			if leftChild.Value == "concat" {
				queryLeftOperandString = buildConcat(databaseType, leftChild)
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
			fmt.Printf("[DEBUG] queryLeftOperandString: %s\n", queryLeftOperandString)
			if strings.Contains(queryLeftOperandString, "/") {
				filterMap := buildNestedFilter(queryLeftOperandString, leftChild, root, gqTranslation)
				db = db.Where(filterMap)
			} else {
				replacementString := "%s LIKE %s"
				if notEnabled {
					replacementString = "%s NOT LIKE %s"
				}
				queryString := fmt.Sprintf(replacementString, queryLeftOperandString, queryRightOperandString)
				db = db.Where(queryString)
			}
		}
	case syntaxtree.UnaryOperator:
		if root.Value != "not" {
			return db, &InvalidQueryError{}
		}
		var err error
		db, err = buildGormQuery(root.LeftChild, db, databaseType, operatorTranslationReversed, gormqonvertTranslationReversed, true)
		if err != nil {
			return db, err
		}
	default:
		return db, &InvalidQueryError{}
	}

	return db, nil
}

func buildNestedFilter(queryRightOperandString string, leftChild *syntaxtree.Node, root *syntaxtree.Node, gqTranslation map[string]string) map[string]any {
	filterMap := map[string]any{}
	currentMap := filterMap
	queryRightOperandString = strings.ReplaceAll(queryRightOperandString, "'", "")
	leftChildValue := leftChild.Value
	fmt.Printf("[DEBUG] leftChildValue: %s\n", leftChildValue)
	functionRegexMatches := unaryFunctionRegex.FindStringSubmatch(leftChildValue)
	fmt.Printf("[DEBUG] functionRegexMatches: %+v\n", functionRegexMatches)
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
			currentMap[fieldSnakeCase] = gqTranslation[root.Value] + currentMap[fieldSnakeCase].(string)
		}
	}

	return filterMap
}

func buildConcat(databaseType DbType, root *syntaxtree.Node) string {
	result := ""
	if root.Value == "concat" {
		result = fmt.Sprintf("%s || %s", buildConcat(databaseType, root.LeftChild), buildConcat(databaseType, root.RightChild))
	}
	if root.Type == syntaxtree.UnaryOperator {
		result = buildUnaryFuncChain(databaseType, root)
	}

	if root.Type == syntaxtree.LeftOperand {
		result = root.Value
		if !strings.Contains(result, "'") {
			result = strcase.SnakeCase(result)
		}
	}

	return result
}

func buildUnaryFuncChain(databaseType DbType, root *syntaxtree.Node) string {
	// TODO: support for nested filters
	result := ""
	nodesVisited := map[int]bool{}
	for !nodesVisited[root.Id] && root.Type == syntaxtree.UnaryOperator {
		if root.LeftChild != nil && root.LeftChild.Type == syntaxtree.UnaryOperator && !nodesVisited[root.LeftChild.Id] {
			root = root.LeftChild
			continue
		}
		nodesVisited[root.Id] = true
		if result == "" {
			leftChildValue := root.LeftChild.Value
			if strings.Contains(leftChildValue, "concat") {
				leftChildValue = buildConcat(databaseType, root.LeftChild)
			} else {
				leftChildValue = strcase.SnakeCase(leftChildValue)
			}
			if strings.Contains(unaryFunctionTranslation[databaseType][root.Value], "%") {
				result = fmt.Sprintf(unaryFunctionTranslation[databaseType][root.Value], leftChildValue)
			} else {
				result = fmt.Sprintf("%s(%s)", unaryFunctionTranslation[databaseType][root.Value], leftChildValue)
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
