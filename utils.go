package database

import "strings"

//QuestionMarks returns a "csv" of question marks
//of length *cnt*. It's designed to make a parameterized
//query work with a slice, e.g.,
//"SELECT * FROM table WHERE ids IN (?, ?, ?), where
//the ?, ?, ? gets replaced by a slice with three items
func (db *DB) QuestionMarks(cnt int) string {

	marks := []string{}
	for i := 0; i < cnt; i++ {
		marks = append(marks, "?")
	}

	return strings.Join(marks, ", ")
}
