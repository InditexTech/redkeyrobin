// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/inditextech/redkeyrobin/internal/util"
	redisgo "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// TestNewRedisClient verifies the creation of a new RedisClient.
func TestNewRedisClient(t *testing.T) {
	ctx := context.Background()
	rc := NewRedisClient(ctx, "localhost", "", 0)

	assert.NotNil(t, rc)
	assert.Equal(t, "localhost:6379", rc.client.Options().Addr)
	assert.Equal(t, "", rc.client.Options().Password)
	assert.Equal(t, 0, rc.client.Options().DB)
}

// TestCheckConnection tests the CheckConnection
func TestCheckConnection(t *testing.T) {
	tests := []struct {
		name               string
		getRedisClientMock func() (*redisgo.Client, redismock.ClientMock)
		maxRetries         int
		backoff            time.Duration
		expectedError      error
	}{
		{
			name: "bad max retries",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				return client, mock
			},
			maxRetries:    0,
			expectedError: fmt.Errorf("maxRetries must be greater than 0"),
		},
		{
			name: "bad backoff",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				return client, mock
			},
			maxRetries:    1,
			backoff:       time.Duration(-1),
			expectedError: fmt.Errorf("backoff must be greater than 0"),
		},
		{
			name: "failed to connect",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectPing().SetErr(errors.New("failed ping"))
				return client, mock
			},
			maxRetries:    1,
			backoff:       time.Microsecond * 10,
			expectedError: fmt.Errorf("failed to connect after 1 retries"),
		},
		{
			name: "success",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectPing().SetVal("PONG")
				return client, mock
			},
			maxRetries:    1,
			backoff:       time.Microsecond * 10,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := tt.getRedisClientMock()
			rc := &RedisClient{
				client: client,
				ctx:    context.Background(),
			}

			err := rc.CheckConnection(tt.maxRetries, tt.backoff)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.Nil(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestCheckConnection tests the GetInfo
func TestGetInfo(t *testing.T) {
	tests := []struct {
		name               string
		getRedisClientMock func() (*redisgo.Client, redismock.ClientMock)
		expectedError      error
		expectedVersion    string
	}{
		{
			name: "failed to get info",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectInfo("all").SetErr(fmt.Errorf("failed info"))
				return client, mock
			},
			expectedError: fmt.Errorf("failed to get info from localhost:6379: failed info"),
		},
		{
			name: "success",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				infoOutput := "# Server\nredis_version:6.2.5\n"
				mock.ExpectInfo("all").SetVal(infoOutput)
				return client, mock
			},
			expectedVersion: "6.2.5",
			expectedError:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := tt.getRedisClientMock()
			rc := &RedisClient{
				client: client,
				ctx:    context.Background(),
			}

			redisInfo, err := rc.GetInfo()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Nil(t, redisInfo)
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, redisInfo)
				assert.Equal(t, tt.expectedVersion, redisInfo.Server["redis_version"])
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestGetClusterInfo test the GetClusterInfo
func TestGetClusterInfo(t *testing.T) {
	tests := []struct {
		name                string
		getRedisClientMock  func() (*redisgo.Client, redismock.ClientMock)
		expectedClusterInfo *ClusterInfo
		expectedError       error
	}{
		{
			name: "failed to get cluster info",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectClusterInfo().SetErr(fmt.Errorf("failed cluster info"))
				return client, mock
			},
			expectedError: fmt.Errorf("failed to get cluster info from localhost:6379: failed cluster info"),
		},
		{
			name: "empty cluster info",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectClusterInfo().SetVal(``)
				return client, mock
			},
			expectedError: fmt.Errorf("empty cluster info response"),
		},
		{
			name: "success",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectClusterInfo().SetVal(`
cluster_state:ok
cluster_slots_assigned:16384
cluster_slots_ok:16384
cluster_slots_pfail:0
cluster_slots_fail:0
cluster_known_nodes:5
cluster_size:5
cluster_current_epoch:13877
cluster_my_epoch:13877
cluster_stats_messages_ping_sent:19544
cluster_stats_messages_pong_sent:52643
cluster_stats_messages_meet_sent:187
cluster_stats_messages_update_sent:6
cluster_stats_messages_sent:72380
cluster_stats_messages_ping_received:19675
cluster_stats_messages_pong_received:72169
cluster_stats_messages_meet_received:188
cluster_stats_messages_fail_received:1
cluster_stats_messages_update_received:4
cluster_stats_messages_received:92037
total_cluster_links_buffer_limit_exceeded:0
fail-line
`)
				return client, mock
			},
			expectedClusterInfo: &ClusterInfo{
				State:                  "ok",
				SlotsAssigned:          16384,
				SlotsOK:                16384,
				SlotsPFail:             0,
				SlotsFail:              0,
				KnownNodes:             5,
				ClusterSize:            5,
				CurrentEpoch:           13877,
				MyEpoch:                13877,
				MessagesPingSent:       19544,
				MessagesPongSent:       52643,
				MessagesMeetSent:       187,
				MessagesUpdateSent:     6,
				MessagesSent:           72380,
				MessagesPingReceived:   19675,
				MessagesPongReceived:   72169,
				MessagesMeetReceived:   188,
				MessagesFailReceived:   1,
				MessagesUpdateReceived: 4,
				MessagesReceived:       92037,
			},
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := tt.getRedisClientMock()
			rc := &RedisClient{
				logger: util.GetLogger("redis-cluster"),
				client: client,
				ctx:    context.Background(),
			}

			clusterInfo, err := rc.GetClusterInfo()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Nil(t, clusterInfo)
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, clusterInfo)
				assert.Equal(t, tt.expectedClusterInfo, clusterInfo)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestCheckConnection tests the GetInfo
func TestGetNodesInfo(t *testing.T) {
	tests := []struct {
		name               string
		getRedisClientMock func() (*redisgo.Client, redismock.ClientMock)
		expectedNodesInfo  []RedisNode
		expectedError      error
	}{
		{
			name: "failed to get cluster nodes info",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectClusterNodes().SetErr(errors.New("failed cluster nodes"))
				return client, mock
			},
			expectedError: fmt.Errorf("failed to get cluster nodes info from localhost:6379: failed cluster nodes"),
		},
		{
			name: "empty cluster nodes info",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectClusterNodes().SetVal(``)
				return client, mock
			},
			expectedError: fmt.Errorf("empty cluster nodes response"),
		},
		{
			name: "success",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectClusterNodes().SetVal(`
222d03eb91487e6542cff1e105d911deb37a5ddd 10.253.43.143:6379@16379 master - 0 1740670560026 13876 connected 9828-10923 12560-13103 14744-16383
77e5805a3550270e5cf23ed42bc2d0577426d876 10.252.6.201:6379@16379 myself,master - 0 1740670560000 13877 disconnected 2456-3275 4912-6277 7914-7917 8738-9553 9558-9827
bb1704c223955cf9a533142e4569f7aba510b1ea 10.252.26.193:6379@16379 slave 77e5805a3550270e5cf23ed42bc2d0577426d876 0 1740670561031 13846 connected 0 1-815 9554-9557 10924-11739 13104-14743
e420256dda2dbfb8db95658397ca8af3c3889b31 10.253.21.209:6379@16379 myself,slave - 0 1740670562035 13852 connected 816-1635 3276-4091 6278-7097 11740-12559
0d691cdfe68b44134f8cdbca0d81563754a5aa6f 10.252.8.20:6379@16379 noaddr - 0 1740670559023 13874 connected 
malformed-line
`)
				mock.ExpectClusterCountFailureReports("222d03eb91487e6542cff1e105d911deb37a5ddd").SetVal(100)
				return client, mock
			},
			expectedNodesInfo: []RedisNode{
				{
					ID:    "222d03eb91487e6542cff1e105d911deb37a5ddd",
					IP:    "10.253.43.143",
					Flags: "master",
					Slots: []RedisSlotRange{
						{
							Start: 9828,
							End:   10923,
						},
						{
							Start: 12560,
							End:   13103,
						},
						{
							Start: 14744,
							End:   16383,
						},
					},
					MasterID:   "-",
					Failures:   100,
					Sent:       0,
					Recv:       1740670560026,
					LinkStatus: "connected",
					Migrating : map[int]string{},
					Importing : map[int]string{},
				},
				{
					ID:    "77e5805a3550270e5cf23ed42bc2d0577426d876",
					IP:    "10.252.6.201",
					Flags: "myself,master",
					Slots: []RedisSlotRange{
						{
							Start: 2456,
							End:   3275,
						},
						{
							Start: 4912,
							End:   6277,
						},
						{
							Start: 7914,
							End:   7917,
						},
						{
							Start: 8738,
							End:   9553,
						},
						{
							Start: 9558,
							End:   9827,
						},
					},
					MasterID:   "-",
					Failures:   0,
					Sent:       0,
					Recv:       1740670560000,
					LinkStatus: "disconnected",
					Migrating : map[int]string{},
					Importing : map[int]string{},
				},
				{
					ID:    "bb1704c223955cf9a533142e4569f7aba510b1ea",
					IP:    "10.252.26.193",
					Flags: "slave",
					Slots: []RedisSlotRange{
						{
							Start: 0,
							End:   0,
						},
						{
							Start: 1,
							End:   815,
						},
						{
							Start: 9554,
							End:   9557,
						},
						{
							Start: 10924,
							End:   11739,
						},
						{
							Start: 13104,
							End:   14743,
						},
					},
					MasterID:   "77e5805a3550270e5cf23ed42bc2d0577426d876",
					Failures:   0,
					Sent:       0,
					Recv:       1740670561031,
					LinkStatus: "connected",
					Migrating : map[int]string{},
					Importing : map[int]string{},
				},
				{
					ID:    "e420256dda2dbfb8db95658397ca8af3c3889b31",
					IP:    "10.253.21.209",
					Flags: "myself,slave",
					Slots: []RedisSlotRange{
						{
							Start: 816,
							End:   1635,
						},
						{
							Start: 3276,
							End:   4091,
						},
						{
							Start: 6278,
							End:   7097,
						},
						{
							Start: 11740,
							End:   12559,
						},
					},
					MasterID:   "-",
					Failures:   0,
					Sent:       0,
					Recv:       1740670562035,
					LinkStatus: "connected",
					Migrating : map[int]string{},
					Importing : map[int]string{},
				},
				{
					ID:         "0d691cdfe68b44134f8cdbca0d81563754a5aa6f",
					IP:         "10.252.8.20",
					Flags:      "noaddr",
					Slots:      []RedisSlotRange{},
					MasterID:   "-",
					Failures:   0,
					Sent:       0,
					Recv:       1740670559023,
					LinkStatus: "connected",
					Migrating : map[int]string{},
					Importing : map[int]string{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := tt.getRedisClientMock()
			rc := &RedisClient{
				logger: util.GetLogger("redis-cluster"),
				client: client,
				ctx:    context.Background(),
			}

			nodesInfo, err := rc.GetNodesInfo()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Nil(t, nodesInfo)
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, nodesInfo)
				assert.Equal(t, tt.expectedNodesInfo, nodesInfo)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestGetMyID tests the GetMyID
func TestGetMyID(t *testing.T) {
	tests := []struct {
		name               string
		getRedisClientMock func() (*redisgo.Client, redismock.ClientMock)
		expectedID         string
		expectedError      error
	}{
		{
			name: "failed to get my ID",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("CLUSTER", "MYID").SetErr(fmt.Errorf("failed ID"))
				return client, mock
			},
			expectedError: fmt.Errorf("failed to get cluster my ID from localhost:6379: failed ID"),
		},
		{
			name: "success",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("CLUSTER", "MYID").SetVal("theawesomeid")
				return client, mock
			},
			expectedID:    "theawesomeid",
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := tt.getRedisClientMock()
			rc := &RedisClient{
				logger: util.GetLogger("redis-cluster"),
				client: client,
				ctx:    context.Background(),
			}

			redisID, err := rc.GetMyID()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Equal(t, redisID, "")
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.expectedID, redisID)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClusterForgetNode(t *testing.T) {
	tests := []struct {
		name               string
		getRedisClientMock func() (*redisgo.Client, redismock.ClientMock)
		expectedError      error
	}{
		{
			name: "failed to forget node",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "forget", "node1").SetErr(fmt.Errorf("failed forget"))
				return client, mock
			},
			expectedError: fmt.Errorf("failed to forget node node1: failed forget"),
		},
		{
			name: "success",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "forget", "node1").SetVal("theawesomeid")
				return client, mock
			},
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := tt.getRedisClientMock()
			rc := &RedisClient{
				logger: util.GetLogger("redis-cluster"),
				client: client,
				ctx:    context.Background(),
			}

			err := rc.ClusterForgetNode("node1")

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.Nil(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClusterMeet(t *testing.T) {
	tests := []struct {
		name               string
		getRedisClientMock func() (*redisgo.Client, redismock.ClientMock)
		expectedError      error
	}{
		{
			name: "failed to meet node",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "meet", "node1", "1234").SetErr(fmt.Errorf("failed meet"))
				return client, mock
			},
			expectedError: fmt.Errorf("failed to meet node node1: failed meet"),
		},
		{
			name: "success",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "meet", "node1", "1234").SetVal("theawesomeid")
				return client, mock
			},
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := tt.getRedisClientMock()
			rc := &RedisClient{
				logger: util.GetLogger("redis-cluster"),
				client: client,
				ctx:    context.Background(),
			}

			err := rc.ClusterMeet("node1", 1234)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.Nil(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClusterReplicate(t *testing.T) {
	tests := []struct {
		name               string
		getRedisClientMock func() (*redisgo.Client, redismock.ClientMock)
		expectedError      error
	}{
		{
			name: "failed to replicate node",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "replicate", "node1").SetErr(fmt.Errorf("failed replicate"))
				return client, mock
			},
			expectedError: fmt.Errorf("failed to replicate node node1: failed replicate"),
		},
		{
			name: "success",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "replicate", "node1").SetVal("theawesomeid")
				return client, mock
			},
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := tt.getRedisClientMock()
			rc := &RedisClient{
				logger: util.GetLogger("redis-cluster"),
				client: client,
				ctx:    context.Background(),
			}

			err := rc.ClusterReplicate("node1")

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.Nil(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClusterReset(t *testing.T) {
	tests := []struct {
		name               string
		getRedisClientMock func() (*redisgo.Client, redismock.ClientMock)
		hard               bool
		expectedError      error
	}{
		{
			name: "failed to reset node",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "reset", "soft").SetErr(fmt.Errorf("failed reset"))
				return client, mock
			},
			expectedError: fmt.Errorf("failed to reset cluster node: failed reset"),
		},
		{
			name: "success",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "reset", "hard").SetVal("theawesomeid")
				return client, mock
			},
			hard:          true,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := tt.getRedisClientMock()
			rc := &RedisClient{
				logger: util.GetLogger("redis-cluster"),
				client: client,
				ctx:    context.Background(),
			}

			err := rc.ClusterReset(tt.hard)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.Nil(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClusterForget(t *testing.T) {
	tests := []struct {
		name               string
		getRedisClientMock func() (*redisgo.Client, redismock.ClientMock)
		expectedError      error
	}{
		{
			name: "failed to forge node",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "forget", "node1").SetErr(fmt.Errorf("failed forget"))
				return client, mock
			},
			expectedError: fmt.Errorf("failed to forget node node1: failed forget"),
		},
		{
			name: "success",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "forget", "node1").SetVal("theawesomeid")
				return client, mock
			},
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := tt.getRedisClientMock()
			rc := &RedisClient{
				logger: util.GetLogger("redis-cluster"),
				client: client,
				ctx:    context.Background(),
			}

			err := rc.ClusterForget("node1")

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.Nil(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClusterAddSlots(t *testing.T) {
	tests := []struct {
		name               string
		getRedisClientMock func() (*redisgo.Client, redismock.ClientMock)
		expectedError      error
	}{
		{
			name: "failed to replicate node",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "addslots", 1, 2, 3).SetErr(fmt.Errorf("failed slots"))
				return client, mock
			},
			expectedError: fmt.Errorf("failed to add slots [1 2 3]: failed slots"),
		},
		{
			name: "success",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "addslots", 1, 2, 3).SetVal("theawesomeid")
				return client, mock
			},
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := tt.getRedisClientMock()
			rc := &RedisClient{
				logger: util.GetLogger("redis-cluster"),
				client: client,
				ctx:    context.Background(),
			}

			err := rc.ClusterAddSlots(1, 2, 3)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.Nil(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClusterFailover(t *testing.T) {
	tests := []struct {
		name               string
		getRedisClientMock func() (*redisgo.Client, redismock.ClientMock)
		expectedError      error
	}{
		{
			name: "failed to replicate node",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "failover").SetErr(fmt.Errorf("failed failover"))
				return client, mock
			},
			expectedError: fmt.Errorf("failed to failover node: failed failover"),
		},
		{
			name: "success",
			getRedisClientMock: func() (*redisgo.Client, redismock.ClientMock) {
				client, mock := redismock.NewClientMock()
				mock.ExpectDo("cluster", "failover").SetVal("theawesomeid")
				return client, mock
			},
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := tt.getRedisClientMock()
			rc := &RedisClient{
				logger: util.GetLogger("redis-cluster"),
				client: client,
				ctx:    context.Background(),
			}

			err := rc.ClusterFailover()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.Nil(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
