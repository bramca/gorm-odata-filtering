package gormodata

type InvalidQueryError struct {
	Msg string
}

func (i *InvalidQueryError) Error() string {
	return "invalid query: " + i.Msg
}
