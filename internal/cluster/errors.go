// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

// OperationInProgressError represents an error when an operation is already in progress.
type OperationInProgressError struct {
	Operation string
}

func (e *OperationInProgressError) Error() string {
	return "operation in progress: " + e.Operation
}

// OperationCompletedError represents an error when an operation is already completed.
type OperationCompletedError struct {
	Operation string
	Reason    string
}

func (e *OperationCompletedError) Error() string {
	return "operation already done: " + e.Operation
}
