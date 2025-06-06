// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package rediscluster

type OperationInProgressError struct {
	Operation string
}

func (e *OperationInProgressError) Error() string {
	return "operation in progress: " + e.Operation
}

type OperationCompletedError struct {
	Operation string
	Reason    string
}

func (e *OperationCompletedError) Error() string {
	return "operation already done: " + e.Operation
}
