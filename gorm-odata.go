package gormodata

import (
	"errors"
	"fmt"
	"regexp"

	syntaxtree "github.com/bramca/go-syntax-tree"
	"gorm.io/gorm"
)

var operatorTranslation = map[string]string{
	"eq": "",
	"ne": "!=",
	"lt": "<",
	"le": "<=",
	"gt": ">",
	"ge": ">=",
}

type OdataQueryBuilder struct {
	OperatorPrecedence []string
	OperatorParsers    []syntaxtree.OperatorParser
	BinaryFunctions    []syntaxtree.BinaryFunctionParser
	UnaryFunctions     []syntaxtree.UnaryFunctionParser
}

func (o *OdataQueryBuilder) NewOdataQueryBuilder() *OdataQueryBuilder {
	o.OperatorPrecedence =  []string{
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

func (o *OdataQueryBuilder) BuildQuery(query string, db *gorm.DB) (*gorm.DB, error) {
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

	fmt.Printf("tree:\n%s\n", tree)

	nodesVisited := map[int]bool{}
	db, nodesVisited, err = buildGormQuery(tree.Root, db, nodesVisited)

	return db, err
}

func buildGormQuery(root *syntaxtree.Node, db *gorm.DB, nodesVisited map[int]bool) (*gorm.DB, map[int]bool, error){
	switch root.Type {
	case syntaxtree.Operator:
		switch root.Value{
		case "and":
			db = db.Where(buildGormQuery(root.LeftChild, db, nodesVisited)).Where(buildGormQuery(root.RightChild, db, nodesVisited))
		case "or":
			db = db.Where(buildGormQuery(root.LeftChild, db, nodesVisited)).Or(buildGormQuery(root.RightChild, db, nodesVisited))
		case "eq", "ne", "lt", "le", "gt", "ge":
			leftChild := root.LeftChild
			operatorVisited := map[int]bool{}
			queryLeftOperandString := ""
			if leftChild.Type == syntaxtree.UnaryOperator {
				for leftChild.Type == syntaxtree.UnaryOperator && operatorVisited[leftChild.Id] {
					if leftChild.LeftChild.Type == syntaxtree.UnaryOperator && !operatorVisited[leftChild.LeftChild.Id] {
						continue
					}
					operatorVisited[leftChild.Id] = true
					if queryLeftOperandString == "" {
						queryLeftOperandString = fmt.Sprintf("%s(%s)", leftChild.Value, leftChild.LeftChild.Value)
					} else {
						queryLeftOperandString = fmt.Sprintf("%s(%s)", leftChild.Value, queryLeftOperandString)
					}
					leftChild = leftChild.Parent

				}
			}
			if leftChild.Type == syntaxtree.LeftOperand {
				queryLeftOperandString = leftChild.Value
				// TODO: if the leftoperand contains an expansion token ('/') then we should create a map according to this format
				// Builds a map in this format
				// Filters: []map[string]interface {}{
				// 	map[string]interface {}{
				// 		"name":[]string{"nc-nsxv-pod5-dcr-red"},
				// 		"network_containers":map[string]interface {}{
				// 			"subnet":[]string{"10.3.112.0"}
				// 		}
				// 	},
				// 	map[string]interface {}{
				// 		"id":"<12"
				// 	},
				// }
			}
			queryString := fmt.Sprintf("%s %s ?", queryLeftOperandString, operatorTranslation[root.Value])
			db = db.Where(queryString, root.RightChild.Value)
		}
	default:
		return db, nodesVisited, errors.New("Invalid query")
	}

	return db, nodesVisited, nil
}
