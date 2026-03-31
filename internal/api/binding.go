package api

import (
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
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

	if isSliceBindingDestination(dest) {
		if err := bindQuerySliceParameter(dest, rawValues); err != nil {
			return fmt.Errorf("invalid query parameter %q: %w", name, err)
		}
		return nil
	}

	if err := bindParameterValue(dest, rawValues[0]); err != nil {
		return fmt.Errorf("invalid query parameter %q: %w", name, err)
	}

	return nil
}

func isSliceBindingDestination(dest any) bool {
	destType := reflect.TypeOf(dest)
	if destType == nil || destType.Kind() != reflect.Ptr {
		return false
	}

	targetType := destType.Elem()
	for targetType.Kind() == reflect.Ptr {
		targetType = targetType.Elem()
	}

	return targetType.Kind() == reflect.Slice
}

func bindQuerySliceParameter(dest any, rawValues []string) error {
	destValue := reflect.ValueOf(dest)
	if !destValue.IsValid() || destValue.Kind() != reflect.Ptr || destValue.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer")
	}

	return assignQuerySlice(destValue.Elem(), rawValues)
}

func bindParameterValue(dest any, raw string) error {
	destValue := reflect.ValueOf(dest)
	if !destValue.IsValid() || destValue.Kind() != reflect.Ptr || destValue.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer")
	}

	return assignBoundValue(destValue.Elem(), raw)
}

func assignQuerySlice(target reflect.Value, rawValues []string) error {
	if target.Kind() == reflect.Ptr {
		bound := reflect.New(target.Type().Elem())
		if err := assignQuerySlice(bound.Elem(), rawValues); err != nil {
			return err
		}
		target.Set(bound)
		return nil
	}

	if target.Kind() != reflect.Slice {
		return fmt.Errorf("unsupported destination type %s", target.Type())
	}

	flattened := make([]string, 0, len(rawValues))
	for _, raw := range rawValues {
		flattened = append(flattened, strings.Split(raw, ",")...)
	}

	slice := reflect.MakeSlice(target.Type(), 0, len(flattened))
	for _, raw := range flattened {
		elem := reflect.New(target.Type().Elem()).Elem()
		if err := assignBoundValue(elem, raw); err != nil {
			return err
		}
		slice = reflect.Append(slice, elem)
	}

	target.Set(slice)
	return nil
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
