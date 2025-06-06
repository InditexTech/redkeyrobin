// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	// "context"
	// "fmt"
	"time"
)

// RemoveOutdatedOperations removes outdated operations from the cluster
func (rc *RedisCluster) RemoveOutdatedOperations() {
	cleanupThreshold := time.Duration(rc.GetReconcilerOperationCleanupInterval()) * time.Second

	for name, operations := range rc.operations {
		for i := len(operations) - 1; i >= 0; i-- {
			if operations[i].GetElapsedTimeFromEnd() > cleanupThreshold {
				rc.operations[name] = append(rc.operations[name][:i], rc.operations[name][i+1:]...)
			}
		}
	}
}

func (rc *RedisCluster) launchOperation(operation RedisOperation, name string, async bool) error {
	// Launch the operation
	err := operation.Launch()
	if err != nil {
		return err
	}
	rc.addOperation(name, operation)

	// Wait for operation to finish synchronously or asynchronously depending on the async flag
	if async {
		go operation.Wait()
	} else {
		return operation.Wait()
	}
	return nil
}

// hasOperation returns true if the cluster has an operation with the specified name and status
func (rc *RedisCluster) hasOperation(name string, status string) bool {
	operations, ok := rc.operations[name]
	if !ok {
		return false
	}

	for _, operation := range operations {
		if operation.GetStatus() == status {
			return true
		}
	}
	return false
}

// hasOperationInNode returns true if the cluster has an operation with the specified name and status in the specified node
func (rc *RedisCluster) hasOperationInNode(name string, status string, node RedisNode) bool {
	operations, ok := rc.operations[name]
	if !ok {
		return false
	}

	for _, operation := range operations {
		if operation.GetStatus() != status {
			continue
		}

		if operation.GetNodeFrom() != nil && operation.GetNodeFrom().Name == node.Name {
			return true
		}
	}
	return false
}

// hasOperationBetweenNodes returns true if the cluster has an operation with the specified name and status between the specified nodes
func (rc *RedisCluster) hasOperationBetweenNodes(name string, status string, from RedisNode, to RedisNode) bool {
	operations, ok := rc.operations[name]
	if !ok {
		return false
	}

	for _, operation := range operations {
		if operation.GetStatus() != status {
			continue
		}

		if operation.GetNodeFrom() == nil || operation.GetNodeTo() == nil {
			continue
		}

		if operation.GetNodeFrom().Name == from.Name && operation.GetNodeTo().Name == to.Name {
			return true
		}
	}
	return false
}

// addOperation adds a new operation to the cluster operations map
func (rc *RedisCluster) addOperation(operationName string, operation RedisOperation) {
	if rc.operations[operationName] == nil {
		rc.operations[operationName] = make([]RedisOperation, 0)
	}

	rc.operations[operationName] = append(rc.operations[operationName], operation)
}

// getOperation returns the operation if the cluster has an operation with the specified name and status or nil otherwise
func (rc *RedisCluster) getOperation(name string, status string) RedisOperation {
	operations, ok := rc.operations[name]
	if !ok {
		return nil
	}

	for _, operation := range operations {
		if operation.GetStatus() == status {
			return operation
		}
	}
	return nil
}
