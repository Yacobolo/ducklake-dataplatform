package api

import (
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"time"
)

var timeType = reflect.TypeOf(time.Time{})

func bindPathParameter(name string, value string, required bool, dest any) error {
	if value == "" {
		if required {
			return fmt.Errorf("missing required path parameter %q", name)
		}
		return nil
	}

	if err := bindParameterValue(dest, value); err != nil {
		return fmt.Errorf("invalid path parameter %q: %w", name, err)
	}

	return nil
}

func bindQueryParameter(values url.Values, name string, required bool, dest any) error {
	rawValues, ok := values[name]
	if !ok || len(rawValues) == 0 {
		if required {
			return fmt.Errorf("missing required query parameter %q", name)
		}
		return nil
	}

	if err := bindParameterValue(dest, rawValues[0]); err != nil {
		return fmt.Errorf("invalid query parameter %q: %w", name, err)
	}

	return nil
}

func bindParameterValue(dest any, raw string) error {
	destValue := reflect.ValueOf(dest)
	if !destValue.IsValid() || destValue.Kind() != reflect.Ptr || destValue.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer")
	}

	return assignBoundValue(destValue.Elem(), raw)
}

func assignBoundValue(target reflect.Value, raw string) error {
	if target.Kind() == reflect.Ptr {
		bound := reflect.New(target.Type().Elem())
		if err := assignBoundValue(bound.Elem(), raw); err != nil {
			return err
		}
		target.Set(bound)
		return nil
	}

	if target.Type() == timeType {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return err
		}
		target.Set(reflect.ValueOf(parsed))
		return nil
	}

	switch target.Kind() {
	case reflect.String:
		target.SetString(raw)
		return nil
	case reflect.Bool:
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		target.SetBool(parsed)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		parsed, err := strconv.ParseInt(raw, 10, target.Type().Bits())
		if err != nil {
			return err
		}
		target.SetInt(parsed)
		return nil
	default:
		return fmt.Errorf("unsupported destination type %s", target.Type())
	}
}
