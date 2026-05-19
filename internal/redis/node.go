// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Node represents a Redis cluster node managed by Robin.
type Node struct {
	// Name is the pod/StatefulSet ordinal name (e.g. "mycluster-0").
	Name string
	// Addr is the node address in host:port format.
	Addr string
	// IP is the node's IP address.
	IP string
	// ID is the cluster node ID obtained from CLUSTER MYID.
	ID string
	// Flags contains the node's cluster flags (e.g. "master", "slave", "myself,master").
	Flags string
	// PrimaryID is the ID of the primary this node replicates, or "-" if it is a primary.
	PrimaryID string
	// Slots is the slot assignment string (e.g. "0-5460").
	Slots string

	client *Client
}

// NewNode creates a new Node with the given name and address.
func NewNode(name, addr, password string) *Node {
	return &Node{
		Name:   name,
		Addr:   addr,
		client: NewClient(addr, password),
	}
}

// Init connects to the node, verifies connectivity with retries, and retrieves
// the cluster node ID. It should be called once after creating the node.
func (n *Node) Init(ctx context.Context, maxRetries int, backoff time.Duration) error {
	if err := n.client.CheckConnection(ctx, maxRetries, backoff); err != nil {
		return fmt.Errorf("node %s: %w", n.Name, err)
	}

	id, err := n.client.ClusterMyID(ctx)
	if err != nil {
		return fmt.Errorf("node %s: %w", n.Name, err)
	}
	n.ID = id

	// Resolve IP from address
	if idx := strings.Index(n.Addr, ":"); idx > 0 {
		n.IP = n.Addr[:idx]
	} else {
		n.IP = n.Addr
	}

	return nil
}

// RefreshInfo updates the node's metadata from the CLUSTER NODES output
// obtained from the node itself.
func (n *Node) RefreshInfo(ctx context.Context) error {
	nodes, err := n.client.GetClusterNodes(ctx)
	if err != nil {
		return fmt.Errorf("node %s: %w", n.Name, err)
	}

	for _, cn := range nodes {
		if cn.ID == n.ID || strings.Contains(cn.Flags, "myself") {
			n.Flags = cn.Flags
			n.PrimaryID = cn.Primary
			n.Slots = cn.Slots
			if cn.IP != "" {
				n.IP = cn.IP
			}
			return nil
		}
	}

	return fmt.Errorf("node %s: could not find self (ID=%s) in CLUSTER NODES output", n.Name, n.ID)
}

// IsPrimary returns true if the node has the "master" flag.
func (n *Node) IsPrimary() bool {
	return strings.Contains(n.Flags, "master")
}

// IsReplica returns true if the node has the "slave" flag.
func (n *Node) IsReplica() bool {
	return strings.Contains(n.Flags, "slave")
}

// Client returns the underlying Redis client for direct command execution.
func (n *Node) Client() *Client {
	return n.client
}

// Close closes the underlying Redis client connection.
func (n *Node) Close() error {
	if n.client != nil {
		return n.client.Close()
	}
	return nil
}
