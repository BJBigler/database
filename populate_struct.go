package database

import (
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/bjbigler/utils"
	"github.com/shopspring/decimal"
)

//StructFromForm populates a struct (ptr, a *pointer*) with http.Request.Form values
//using as keys either "db" or "form" struct tags (e.g., `db:"sql_field_name"` or `form:"customName"`).
//The "form" tag overrides "db".
//HTML input / select / textarea / etc. `name` attributes should match whichever tag value used,
//and can be mixed. Struct field types populated are limted to the following:
//1) Primitives: string, int, int32, int64, float32, and float64;
//2) sqlutils (custom): NullString, NullInt64, NullDecimal, NullBool, NullFloat64, and NullTime.
//The form is parsed (errors ignored) if it comes in nil. Keys not present in the form are
//ignored unless processAllKeys is true. In this case, fields with missing keys are set to the
//type's nil value. This option might be used when the form has many checkboxes, which don't
//send keys when false.
//
//If nil, *location* is set to America/New_York
func StructFromForm(ptr interface{}, r *http.Request, processAllKeys bool, location *time.Location) error {

	if reflect.ValueOf(ptr).Kind() != reflect.Ptr {
		return fmt.Errorf("expected pointer to struct, got struct")
	}

	if r.Form == nil {
		r.ParseForm()
		r.ParseMultipartForm(1 * 1024 * 1024)
	}

	fields := reflect.ValueOf(ptr).Elem()

	for i := 0; i < fields.NumField(); i++ {

		//Get the struct field
		field := fields.Type().Field(i)

		//From the field, get the db tag name or form tag.
		//Prefer form tag over field tag
		key := field.Tag.Get("db")
		formTag := field.Tag.Get("form")
		if formTag != "" {
			key = formTag
		}

		//If the db tag has a hyphen, the value is skipped
		if key == "-" {
			continue
		}

		//Look in the form for the db tag
		postedValues, ok := r.Form[key]

		if !ok && !processAllKeys {
			//The key isn't present; go to next
			continue
		}

		if len(postedValues) == 0 {
			continue
		}

		postedValue := postedValues[0]

		//Convert the value from a string to the field's data type
		var value interface{}

		switch field.Type.Name() {
		case reflect.TypeOf(int(0)).Name():
			value = utils.ParseInt(postedValue, 0)

		case reflect.TypeOf(int32(0)).Name():
			value = utils.ParseInt32(postedValue, 0)

		case reflect.TypeOf(int64(0)).Name():
			value = utils.ParseInt64(postedValue, 0)

		case reflect.TypeOf(float32(0)).Name():
			value = utils.ParseFloat32(postedValue)

		case reflect.TypeOf(float64(0)).Name():
			value = utils.ParseFloat64(postedValue)

		case reflect.TypeOf(time.Time{}).Name():
			value = utils.ParseFloat64(postedValue)

		case reflect.TypeOf(string("")).Name():
			value = postedValue

		case reflect.TypeOf(NullString{}).Name():
			value = GetNullString(postedValue)

		case reflect.TypeOf(NullInt64{}).Name():
			int64Value := utils.ParseInt64(postedValue, 0)
			value = GetNullInt64(int64Value)

		case reflect.TypeOf(NullBool{}).Name():
			boolValue := utils.ValToBool(postedValue)
			value = GetNullBool(boolValue)

		case reflect.TypeOf(NullDecimal{}).Name():
			decimalValue, _ := decimal.NewFromString(postedValue)
			value = GetNullDecimal(decimalValue)

		case reflect.TypeOf(NullFloat64{}).Name():
			floatValue := utils.ParseFloat64(postedValue)
			value = GetNullFloat64(floatValue)

		case reflect.TypeOf(NullTime{}).Name():
			if location == nil {
				location = loc()
			}

			dateValue := utils.ParseDateMulti(postedValue, location)
			value = GetNullTime(dateValue, location)
		}

		//Couldn't figure out how to convert the value, so skip it
		if value == nil {
			continue
		}

		//Assign the field's value
		setField := reflect.ValueOf(ptr).Elem().FieldByName(field.Name)
		if setField.CanSet() && setField.IsValid() {
			setField.Set(reflect.ValueOf(value))
		}
	}

	return nil
}

//Loc returns New York location
func loc() *time.Location {

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		return time.UTC
	}

	return loc
}
