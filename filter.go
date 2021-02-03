package database

//Filter used to construct WHERE clause
type Filter struct {
	Field       string //Struct field (convert to interface{} to hold field value, and then get db tag)
	Value       string //Filter value
	Conjunction string //Should be AND or OR
}

//PrepWhere uses a slice of Filter to create a where clause
func PrepWhere(filters []*Filter) (result string) {

	for _, f := range filters {

		//TODO: need to check Field datatype and use quotes only when string or time
		result += f.Field + " = '" + f.Value + "'"
	}

	return result
}
