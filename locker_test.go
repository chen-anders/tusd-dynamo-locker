package dynamolocker

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"testing"
	"time"

	tusd "github.com/tus/tusd/pkg/handler"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestDynamoLocker(t *testing.T) {
	a := assert.New(t)
	customLeaseDuration := int64(1000)
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("us-west-2"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000"}, nil
			})),
	)

	if err != nil {
		t.Fatalf("failed to connect to local dynamoDB: %v", err)
	}
	dynamoDBClient := dynamodb.NewFromConfig(cfg)
	tableName := uuid.New().String()
	locker, err := NewWithLeaseDuration(dynamoDBClient, tableName, customLeaseDuration)
	a.NoError(err)

	capacityUnits := int64(5)
	dynamoDBTableOpts := &DynamoDBTableOptions{
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  &capacityUnits,
			WriteCapacityUnits: &capacityUnits,
		},
	}

	_, err = locker.CreateDynamoDBTable(dynamoDBTableOpts)
	a.NoError(err)

	locker2, err := NewWithLeaseDuration(dynamoDBClient, tableName, customLeaseDuration)
	a.NoError(err)

	l, err := locker.NewLock("one")
	a.NoError(err)
	a.NoError(l.Lock())
	a.Equal(tusd.ErrFileLocked, l.Lock())
	time.Sleep(2 * time.Second)
	// test that lock remains between heartbeats
	a.Equal(tusd.ErrFileLocked, l.Lock())
	// test that the lock cannot be taken by a second client
	l2, err := locker2.NewLock("one")
	a.NoError(err)
	a.Equal(tusd.ErrFileLocked, l2.Lock())
	a.NoError(l.Unlock())
	a.Equal(ErrLockNotHeld, l.Unlock())
	a.NoError(l2.Lock())
	a.Equal(tusd.ErrFileLocked, l2.Lock())
	a.NoError(l2.Unlock())
	a.Equal(ErrLockNotHeld, l2.Unlock())
}
