// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"
	"net/http"
	"reflect"
)

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