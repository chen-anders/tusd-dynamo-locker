package dynamolocker

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/tus/tusd"
	"github.com/twinj/uuid"
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
	tableName := uuid.NewV4().String()
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

	a.NoError(locker.LockUpload("one"))
	a.Equal(tusd.ErrFileLocked, locker.LockUpload("one"))
	time.Sleep(2 * time.Second)
	// test that lock remains between heartbeats
	a.Equal(tusd.ErrFileLocked, locker.LockUpload("one"))
	// test that the lock cannot be taken by a second client
	a.Equal(tusd.ErrFileLocked, locker2.LockUpload("one"))
	a.NoError(locker.UnlockUpload("one"))
	a.Equal(ErrLockNotHeld, locker.UnlockUpload("one"))
	a.NoError(locker2.LockUpload("one"))
	a.Equal(tusd.ErrFileLocked, locker2.LockUpload("one"))
	a.NoError(locker2.UnlockUpload("one"))
	a.Equal(ErrLockNotHeld, locker2.UnlockUpload("one"))
}
