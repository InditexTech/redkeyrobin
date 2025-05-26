// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOperationWait(t *testing.T) {
	tests := []struct {
		name           string
		operation      *RedisOperation
		expectedStatus string
		expectedError  error
	}{
		{
			name: "error",
			operation: &RedisOperation{
				Cmd: NewRedisCLICommand(t.Context(), "exit 1"),
			},
			expectedStatus: "Error",
			expectedError:  fmt.Errorf("Command failed with exit code 1"),
		},
		{
			name: "good",
			operation: &RedisOperation{
				Cmd: NewRedisCLICommand(t.Context(), "exit 0"),
			},
			expectedStatus: "Finished",
			expectedError:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.operation.Cmd.Start()
			err := tt.operation.Wait()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.IsType(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expectedStatus, tt.operation.Status)
			assert.NotNil(t, tt.operation.EndTimestamp)
		})
	}
}
