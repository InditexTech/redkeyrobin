// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

// ParseInt safely converts a string to an int.
func ParseInt(value string) int {
	trimmed := strings.TrimSpace(value)
	num, err := strconv.Atoi(trimmed)
	if err != nil {
		fmt.Printf("could not parse int from %q: %v\n", trimmed, err)
		return 0
	}
	return num
}

// ParseInt64 safely converts a string to an int64.
func ParseInt64(value string) int64 {
	trimmed := strings.TrimSpace(value)
	num, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		fmt.Printf("could not parse int64 from %q: %v\n", trimmed, err)
		return 0
	}
	return num
}

// ParseFloat safely converts a string to a float64.
func ParseFloat(value string) float64 {
	trimmed := strings.TrimSpace(value)
	num, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		fmt.Printf("could not parse float64 from %q: %v\n", trimmed, err)
		return 0.0
	}
	return num
}

// MapToString converts a map[string]string to a string.
func MapToString(m map[string]string) string {
	var b strings.Builder
	b.WriteString("{")
	for k, v := range m {
		b.WriteString(k)
		b.WriteString(": ")
		b.WriteString(v)
		b.WriteString(", ")
	}
	b.WriteString("}")
	return b.String()
}

func MethodIsValid(s any, name string) error {
	// Get the method by name
	method := reflect.ValueOf(s).MethodByName(name)
	if !method.IsValid() {
		return fmt.Errorf("method %s not found", name)
	}

	// Check if the method has the correct signature
	_, ok := method.Interface().(func(http.ResponseWriter, *http.Request))
	if !ok {
		return fmt.Errorf("method %s has invalid signature", name)
	}
	return nil
}

func Invoke(s any, name string, args ...interface{}) []reflect.Value {
	// Compose the arguments
	inputs := make([]reflect.Value, len(args))
	for i, _ := range args {
		inputs[i] = reflect.ValueOf(args[i])
	}

	// Call the method
	method := reflect.ValueOf(s).MethodByName(name)
	return method.Call(inputs)
}

func StringInSlice(item string, list []string) bool {
	for _, i := range list {
		if i == item {
			return true
		}
	}
	return false
}
