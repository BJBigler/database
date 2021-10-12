package database

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/shopspring/decimal"
)

//GetNullInt64 returns sqlutils.NullInt64 with the supplied value
func GetNullInt64(value int64) NullInt64 {
	return NullInt64{Valid: true, Int64: value}
}

//GetNullFloat64 returns sqlutils.NullFloat64 with the supplied value
func GetNullFloat64(value float64) NullFloat64 {
	return NullFloat64{Valid: true, Float64: value}
}

//GetNullString ...
func GetNullString(value string) NullString {
	return NullString{Valid: true, String: value}
}

//GetNullBool ...
func GetNullBool(value bool) NullBool {
	return NullBool{Valid: true, Bool: value}
}

//GetNullTime ...
func GetNullTime(value time.Time, location *time.Location) NullTime {
	return NullTime{Valid: true, Time: value, Location: location}
}

//GetNullDecimal ...
func GetNullDecimal(value decimal.Decimal) NullDecimal {
	return NullDecimal{Valid: true, Decimal: value}
}

//NullTime ...
type NullTime struct {
	Time     time.Time      //
	Valid    bool           // Valid is true if Time is not NULL
	Location *time.Location //UTC if not specified
}

// Scan implements the Scanner interface.
// The value type must be time.Time or string / []byte (formatted time-string),
// otherwise Scan fails.
func (nt *NullTime) Scan(value interface{}) (err error) {

	if value == nil {
		nt.Time, nt.Valid = time.Time{}, false
		return
	}

	if nt.Location == nil {
		//nt.Location = time.UTC
		nt.Location, err = time.LoadLocation("America/New_York")
		if err != nil {
			nt.Location = time.UTC
		}
	}
	//loc, _ := time.LoadLocation("America/Los_Angeles") //Changed back to UTC on 2018-06-26

	switch v := value.(type) {
	case time.Time:
		nt.Time, nt.Valid = v, true
		return
	case []byte:

		parseValue := string(v)
		// if len(parseValue) == 10 {
		// 	//If the value doesn't have any time information,
		// 	//parse it in New York
		// 	nt.Location, _ = time.LoadLocation("America/New_York")
		// }

		nt.Time, err = parseDateTime(parseValue, nt.Location)
		nt.Valid = (err == nil)
		return
	case string:
		if len(v) == 10 {
			//If the value doesn't have any time information,
			//parse it in New York
			nt.Location, _ = time.LoadLocation("America/New_York")
		}

		nt.Time, err = parseDateTime(v, nt.Location)
		nt.Valid = (err == nil)
		return
	}
	nt.Valid = false

	return fmt.Errorf("can't convert %T to time.Time", value)
}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

func parseDateTime(str string, loc *time.Location) (t time.Time, err error) {
	base := "0000-00-00 00:00:00.0000000"

	timeFormat := "2006-01-02 15:04:05.999999"

	switch len(str) {
	case 10, 19, 21, 22, 23, 24, 25, 26: // up to "YYYY-MM-DD HH:MM:SS.MMMMMM"
		if str == base[:len(str)] {
			return
		}
		t, err = time.Parse(timeFormat[:len(str)], str)
	default:
		err = fmt.Errorf("invalid time string: %s", str)
		return
	}

	// Adjust location
	if err == nil && loc != time.UTC {
		y, mo, d := t.Date()
		h, mi, s := t.Clock()
		t, err = time.Date(y, mo, d, h, mi, s, t.Nanosecond(), loc), nil
	}

	return
}

//NullDecimal ...
type NullDecimal struct {
	Decimal decimal.Decimal
	Valid   bool
}

// Scan implements the Scanner interface.
func (nd *NullDecimal) Scan(value interface{}) error {
	if value == nil {
		nd.Decimal, nd.Valid = decimal.New(0, 1), false
		return nil
	}
	nd.Valid = true

	var result error
	//The sql driver delivers decimals from the DB as uint8.
	//Here, we cast those to strings, and then use the decimal library
	//to convert them to a decimal.
	stringToConvert := string(value.([]uint8))

	nd.Decimal, result = decimal.NewFromString(stringToConvert)

	return result

}

// Value implements the driver Valuer interface.
func (nd NullDecimal) Value() (driver.Value, error) {
	if !nd.Valid {
		return nil, nil
	}
	return nd.Decimal, nil
}

// CUSTOM NULL Handling structures

// NullInt64 is an alias for sql.NullInt64 data type
type NullInt64 sql.NullInt64

// Scan implements the Scanner interface for NullInt64
func (ni *NullInt64) Scan(value interface{}) error {
	var i sql.NullInt64
	if err := i.Scan(value); err != nil {
		return err
	}

	// if nil then make Valid false
	if reflect.TypeOf(value) == nil {
		*ni = NullInt64{i.Int64, false}
	} else {
		*ni = NullInt64{i.Int64, true}
	}
	return nil
}

// Value implements the driver Valuer interface.
func (ni NullInt64) Value() (driver.Value, error) {
	if !ni.Valid {
		return nil, nil
	}
	return ni.Int64, nil
}

// NullBool is an alias for sql.NullBool data type
type NullBool sql.NullBool

// Scan implements the Scanner interface for NullBool
func (nb *NullBool) Scan(value interface{}) error {
	var b sql.NullBool
	if err := b.Scan(value); err != nil {
		return err
	}

	// if nil then make Valid false
	if reflect.TypeOf(value) == nil {
		*nb = NullBool{b.Bool, false}
	} else {
		*nb = NullBool{b.Bool, true}
	}

	return nil
}

// Value implements the driver Valuer interface.
func (nb NullBool) Value() (driver.Value, error) {
	if !nb.Valid {
		return nil, nil
	}
	return nb.Bool, nil
}

// NullFloat64 is an alias for sql.NullFloat64 data type
type NullFloat64 sql.NullFloat64

// Scan implements the Scanner interface for NullFloat64
func (nf *NullFloat64) Scan(value interface{}) error {
	var f sql.NullFloat64
	if err := f.Scan(value); err != nil {
		return err
	}

	// if nil then make Valid false
	if reflect.TypeOf(value) == nil {
		*nf = NullFloat64{f.Float64, false}
	} else {
		*nf = NullFloat64{f.Float64, true}
	}

	return nil
}

// Value implements the driver Valuer interface.
func (nf NullFloat64) Value() (driver.Value, error) {
	if !nf.Valid {
		return nil, nil
	}
	return nf.Float64, nil
}

// NullString is an alias for sql.NullString data type
type NullString sql.NullString

// Scan implements the Scanner interface for NullString
func (ns *NullString) Scan(value interface{}) error {
	var s sql.NullString
	if err := s.Scan(value); err != nil {
		return err
	}

	// if nil then make Valid false
	if reflect.TypeOf(value) == nil {
		*ns = NullString{s.String, false}
	} else {
		*ns = NullString{s.String, true}
	}

	return nil
}

// Value implements the driver Valuer interface.
func (ns NullString) Value() (driver.Value, error) {
	if !ns.Valid {
		return nil, nil
	}
	return ns.String, nil
}

// MarshalJSON for NullInt64
func (ni *NullInt64) MarshalJSON() ([]byte, error) {
	if !ni.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(ni.Int64)
}

// UnmarshalJSON for NullInt64
func (ni *NullInt64) UnmarshalJSON(b []byte) error {
	err := json.Unmarshal(b, &ni.Int64)
	ni.Valid = (err == nil)
	return err
}

// MarshalJSON for NullBool
func (nb *NullBool) MarshalJSON() ([]byte, error) {
	if !nb.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(nb.Bool)
}

// UnmarshalJSON for NullBool
func (nb *NullBool) UnmarshalJSON(b []byte) error {
	err := json.Unmarshal(b, &nb.Bool)
	nb.Valid = (err == nil)
	return err
}

// MarshalJSON for NullFloat64
func (nf *NullFloat64) MarshalJSON() ([]byte, error) {
	if !nf.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(nf.Float64)
}

// UnmarshalJSON for NullFloat64
func (nf *NullFloat64) UnmarshalJSON(b []byte) error {
	err := json.Unmarshal(b, &nf.Float64)
	nf.Valid = (err == nil)
	return err
}

// MarshalJSON for NullString
func (ns *NullString) MarshalJSON() ([]byte, error) {
	if !ns.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(ns.String)
}

// UnmarshalJSON for NullString
func (ns *NullString) UnmarshalJSON(b []byte) error {
	err := json.Unmarshal(b, &ns.String)
	ns.Valid = (err == nil)
	return err
}

// MarshalJSON for NullTime
func (nt *NullTime) MarshalJSON() ([]byte, error) {
	if !nt.Valid {
		return []byte("null"), nil
	}
	val := fmt.Sprintf("\"%s\"", nt.Time.Format(time.RFC3339))
	return []byte(val), nil
}

// UnmarshalJSON for NullTime
func (nt *NullTime) UnmarshalJSON(b []byte) error {
	s := string(b)
	// s = Stripchars(s, "\"")

	x, err := time.Parse(time.RFC3339, s)
	if err != nil {
		nt.Valid = false
		return err
	}

	nt.Time = x
	nt.Valid = true
	return nil
}
