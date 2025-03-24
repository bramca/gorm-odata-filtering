package gormodata

type InvalidQueryError struct{}

func (i *InvalidQueryError) Error() string {
	return "invalid query"
}
