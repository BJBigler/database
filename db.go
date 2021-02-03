package database

import (
	//"fmt"
	"crypto/rand"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/bjbigler/utils"
	"github.com/shopspring/decimal"

	//Imported to initialize the sqlx package
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

//DB ...
type DB struct {
	*sqlx.DB
}

//New ...
func New(db *sqlx.DB) *DB {
	// Configure any package-level settings
	return &DB{db}
}

//Connect returns a database connection
func Connect(conn string) *DB {
	connection, err := sqlx.Open("mysql", conn)

	if err != nil {
		utils.Log(err)
		return nil
	}
	db := New(connection)
	return db
}

//Named is used to provide a SQL statement and
//struct for an ExecNamed execution
type Named struct {
	SQL       string
	StructVal interface{}
}

//ExecList takes a slice (list) of SQL commands
//and executes them in batches of 200.
func (db *DB) ExecList(sqlList []string) (errors []error) {
	if len(sqlList) == 0 {
		return
	}

	//defer db.Close()

	cnt := 0

	sqlMultiStatement := ""

	for _, sql := range sqlList {

		cnt++

		//Append semicolon to each string where missing
		if !strings.HasSuffix(sql, ";") {
			sql += ";"
		}

		sqlMultiStatement += sql

		if cnt == 200 {
			cnt = 0
			_, err := db.Exec(sqlMultiStatement)
			if err != nil {
				utils.Log("Could not execute statement: " + sqlMultiStatement)
				utils.Log(err)
				errors = append(errors, err)
			}

			sqlMultiStatement = ""
		}
	}

	//Execute remaining statements, if any
	if sqlMultiStatement != "" {
		_, err := db.Exec(sqlMultiStatement)
		if err != nil {
			utils.Log("Could not execute statement: " + sqlMultiStatement)
			utils.Log(err)
			errors = append(errors, err)
		}
	}

	return errors
}

//ExecNamedList ...
func (db *DB) ExecNamedList(namedList []*Named) []error {
	//utils.Log("Starting exec named list")
	var errors []error

	if len(namedList) > 20 {
		return []error{fmt.Errorf("Sql statements exceeded 20, aborting")}
	}

	//db = sqlx.MustConnect("mysql", conn)
	//defer db.Close()

	//utils.Log(fmt.Sprintf("Executing %v statements", len(namedList)))
	for _, s := range namedList {
		_, err := db.NamedExec(s.SQL, s.StructVal)

		if err != nil {
			utils.Log(fmt.Sprintf("%v\n%v", err, s.SQL))
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return errors
	}

	return nil

}

//ExecNamedListAsTransaction ...
func (db *DB) ExecNamedListAsTransaction(namedList []*Named) []error {
	var errors []error

	if len(namedList) > 20 {
		return []error{fmt.Errorf("Sql statements exceeded 20, aborting")}
	}

	//db = sqlx.MustConnect("mysql", conn)
	//defer db.Close()

	tx := db.MustBegin()

	for _, s := range namedList {
		_, err := tx.NamedExec(s.SQL, s.StructVal)

		if err != nil {
			tx.Rollback()
			utils.Log(fmt.Sprintf("%v\n%v", err, s.SQL))
			errors = append(errors, err)
			return errors
		}
	}

	err := tx.Commit()

	if err != nil {
		tx.Rollback()
		utils.Log(err)
		errors = append(errors, err)
		return errors
	}

	return nil

}

//ExecListAsTransaction executes a set of SQL statements in a transaction.
//Statement count should not exceed 20.
func (db *DB) ExecListAsTransaction(sql []string) error {
	if len(sql) > 20 {
		return fmt.Errorf("Sql statements exceeded 20, aborting")
	}

	//db = sqlx.MustConnect("mysql", conn)
	//defer db.Close()

	tx := db.MustBegin()

	for _, s := range sql {
		_, err := tx.Exec(s)

		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err := tx.Commit()

	if err != nil {
		tx.Rollback()
		return err
	}

	return nil
}

//GetRows ...
func (db *DB) GetRows(parseRows func(*sqlx.Rows), sql string, sqlArgs ...interface{}) error {

	if db == nil {
		return fmt.Errorf("db connection was nil")
	}

	if sql == "" {
		return fmt.Errorf("SQL was blank")
	}

	// db, err := sqlx.Connect("mysql", conn)
	// if err != nil {
	// 	return err
	// }

	// defer db.Close()
	// setConnections(db)

	rows, err := db.Queryx(sql, sqlArgs...)
	//Check the error before closing the rows!
	if err != nil {
		//utils.Log(fmt.Sprintf("%v\n%v", err, sql))
		return fmt.Errorf("error GetRows(): %v", err)
	}

	defer rows.Close()

	if rows == nil {
		//utils.Log(fmt.Sprintf("No rows returned with SQL: \n%s", sql))
		return fmt.Errorf("no rows returned with SQL: %s", sql)
	}

	parseRows(rows)

	return err
}

//GetRowsFromNamed is used mostly to filter rows by values in a "dummy" struct object.
//The input SQL must contain SQL_CALC_FOUND_ROWS as its first value.
func (db *DB) GetRowsFromNamed(parseRows func(*sqlx.Rows), sql string, arg interface{}) int {
	// db = sqlx.MustConnect("mysql", conn)

	// defer db.Close()
	// setConnections(db)

	rows, err := db.NamedQuery(sql, arg)
	//Check error before closing rows!
	if err != nil {
		utils.Log(fmt.Sprintf("%v\n%v", err, sql))
		panic(err)
	}

	defer rows.Close()

	parseRows(rows)

	return 0 //TODO: return row count

}

//GetRowsInQuery requires that sql has an IN statement and array
//has the variables the IN statement can use.
//Example: "SELECT * FROM table WHERE t_field IN (?)"
//The "?" is replaced with the values in array.
//See http://jmoiron.github.io/sqlx/
func (db *DB) GetRowsInQuery(parseRows func(*sqlx.Rows), sql string, array interface{}) {

	// db = sqlx.MustConnect("mysql", conn)
	// defer db.Close()
	// setConnections(db)

	query, args, err := sqlx.In(sql, array)
	query = db.Rebind(query)
	rows, err := db.Queryx(query, args...)
	//Check error before closing rows!
	if err != nil {
		utils.Log(fmt.Sprintf("%v\n%v", err, sql))
		panic(err)
	}

	defer rows.Close()

	parseRows(rows)

}

//ExecNamed executes the query provided using the struct for values
func (db *DB) ExecNamed(sql string, structVal interface{}) (sql.Result, error) {

	// db = sqlx.MustConnect("mysql", conn)
	// defer db.Close()
	// setConnections(db)

	result, err := db.NamedExec(sql, structVal)

	if err != nil {
		utils.Log(fmt.Sprintf("Named exec error\nSQL:%v\nError: %v", sql, err))
	}

	return result, err

}

//ExecSingle processes a single sql statement
func (db *DB) ExecSingle(sql string, args ...interface{}) (sql.Result, error) {
	// db = sqlx.MustConnect("mysql", conn)
	// defer db.Close()
	// setConnections(db)

	result, err := db.Exec(sql, args...)
	return result, err
}

//ExecPrepared ...
func (db *DB) ExecPrepared(sql string, args ...interface{}) (sql.Result, error) {
	// db = sqlx.MustConnect("mysql", conn)
	// defer db.Close()
	// setConnections(db)

	return db.Exec(sql, args...)
}

//Prepared ...
type Prepared struct {
	SQL  string
	Args []interface{}
}

//ExecPreparedList ...
func (db *DB) ExecPreparedList(statements []Prepared) {

	// db = sqlx.MustConnect("mysql", conn)
	// defer db.Close()
	// setConnections(db)

	for _, p := range statements {
		db.MustExec(p.SQL, p.Args)
	}
}

//ToInt64ForStorage multiplies the input number by precision
//and returns an int64 for Datastore persist
func ToInt64ForStorage(number decimal.Decimal, precision int32) int64 {

	decimalMultiplier := decimal.New(1, precision)
	number = number.Mul(decimalMultiplier)

	number = number.Round(0)
	return number.IntPart()
}

//FloatToInt64ForStorage ...
func FloatToInt64ForStorage(number float64, precision float64) int64 {
	number = number * math.Pow(10, precision)
	return int64(number)
}

//NewKey generates a new key to serve as primary.
func NewKey() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// generate 64 character string
	bytes := make([]byte, 8)
	rand.Read(bytes)

	for k, v := range bytes {
		bytes[k] = alphabet[v%byte(len(alphabet))]
	}

	return string(bytes)
}

//Sqlize escapes a string for SQL. Use sparingly
func Sqlize(s string) string {
	replace := map[string]string{"\\": "\\\\", "'": `''`, "\\0": "\\\\0", "\n": "\\n", "\r": "\\r", `"`: `\"`, "\x1a": "\\Z"}

	for b, a := range replace {
		s = strings.Replace(s, b, a, -1)
	}

	return s
}

//Int64Scalar returns an int64 from the first field named *result* from the first result row.
//NOTE: *The field MUST be named "result" and MUST be coerceable into an int64.* If the
//statment returns more than one row, only the first row is used.
func (db *DB) Int64Scalar(sqlStr string, args ...interface{}) (int64, error) {

	var result int64
	var err error

	// db = sqlx.MustConnect("mysql", conn)
	// defer db.Close()
	// setConnections(db)

	row := db.QueryRow(sqlStr, args...)

	switch err = row.Scan(&result); err {
	case sql.ErrNoRows:
		utils.Log("No rows in scalar")
		return 0, fmt.Errorf("no rows returned")
	case nil:
		return result, nil
	default:
		utils.Log(err)
		return 0, err
	}
}

func setConnections(db *sqlx.DB) {
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
}
