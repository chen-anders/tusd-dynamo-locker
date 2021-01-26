package dynamolocker

import (
	"testing"
	"time"

	tusd "github.com/tus/tusd/pkg/handler"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestDynamoLocker(t *testing.T) {
	a := assert.New(t)
	customLeaseDuration := int64(1000)
	sess, err := session.NewSession(&aws.Config{
		Region:   aws.String("us-west-2"),
		Endpoint: aws.String("http://localhost:8000"),
	})
	if err != nil {
		t.Fatalf("failed to connect to local dynamoDB: %v", err)
	}
	dbSvc := dynamodb.New(sess)
	tableName := uuid.New().String()
	locker, err := NewWithLeaseDuration(dbSvc, tableName, customLeaseDuration)
	a.NoError(err)

	dynamoDBTableOpts := &DynamoDBTableOptions{
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	}

	_, err = locker.CreateDynamoDBTable(dynamoDBTableOpts)
	a.NoError(err)

	locker2, err := NewWithLeaseDuration(dbSvc, tableName, customLeaseDuration)
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
