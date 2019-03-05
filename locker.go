// Package dynamolocker provides a locking mechanism using AWS DynamoDB
//
// To initialize a locker, a pre-existing connected DynamoDB client must be present
//
//	import (
//		dynamolocker "github.com/chen-anders/tusd-dynamo-locker"
//	)
//
//	dynamoDBClient := dynamodb.New(session.New(), &aws.Config{
//		Region:   aws.String("us-west-2"),
//	})
//
//	locker, err := dynamolocker.New(dyanmoDBClient, "my-locker")
//	if err != nil {
//		log.Fatal(err)
//	}
//
// The locker will need to be included in composer that is used by tusd:
//
//	composer := tusd.NewStoreComposer()
//	locker.UseIn(composer)
//
// A custom lease duration (in milliseconds) can be specified using
// dynamolocker.NewWithLeaseDuration:
//
//	dynamoDBClient := dynamodb.New(session.New(), &aws.Config{
//		Region:   aws.String("us-west-2"),
//	})
//
//	thirtySeconds := int64(30000)
//	locker, err := dynamolocker.NewWithLeaseDuration(dyanmoDBClient, "my-locker", thirtySeconds)
//	if err != nil {
//		log.Fatal(err)
//	}
//
package dynamolocker

import (
	"errors"
	"log"
	"sync"
	"time"

	"cirello.io/dynamolock"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/tus/tusd"
)

const DEFAULT_LEASE_DURATION_MILLISECONDS = int64(60000)

var (
	ErrLockNotHeld = errors.New("Lock not held")
)

type DynamoDBLocker struct {
	Client *dynamolock.Client

	// locks is used for storing dynamo locks before they are
	// unlocked. If you want to release a lock, you need the same locker
	// instance and therefore we need to save them temporarily.
	locks         map[string]*dynamolock.Lock
	mutex         sync.Mutex
	LeaseDuration int64
	TableName     string
}

// DynamoDBTableOptions is only used for new DynamoDB table creation
// if a table does not already exist
type DynamoDBTableOptions struct {
	// If no ProvisionedThroughput is specified, we create a DynamoDB table with on-demand capacity
	ProvisionedThroughput *dynamodb.ProvisionedThroughput
}

// New constructs a new locker using dynamolockerided client.
func New(client *dynamodb.DynamoDB, tableName string) (*DynamoDBLocker, error) {
	return NewWithLeaseDuration(client, tableName, DEFAULT_LEASE_DURATION_MILLISECONDS)
}

// This method may be used if a custom lease duration is required
func NewWithLeaseDuration(client *dynamodb.DynamoDB, tableName string, leaseDuration int64) (*DynamoDBLocker, error) {
	dynamolockClient, err := dynamolock.New(
		client,
		tableName,
		dynamolock.WithLeaseDuration(time.Duration(leaseDuration)*time.Millisecond),
		dynamolock.WithHeartbeatPeriod(time.Duration(leaseDuration/3)*time.Millisecond),
	)

	if err != nil {
		return nil, err
	}

	lockMap := map[string]*dynamolock.Lock{}
	return &DynamoDBLocker{
		Client:        dynamolockClient,
		locks:         lockMap,
		mutex:         sync.Mutex{},
		LeaseDuration: leaseDuration,
		TableName:     tableName,
	}, nil
}

// CreateDynamoDBTable can be used to setup the DynamoDB table required
// for this locker to work. DynamoDB tables take a few minutes to provision.
func (locker *DynamoDBLocker) CreateDynamoDBTable(options *DynamoDBTableOptions) (*dynamodb.CreateTableOutput, error) {
	if options.ProvisionedThroughput != nil {
		return locker.Client.CreateTable(
			locker.TableName,
			dynamolock.WithProvisionedThroughput(options.ProvisionedThroughput),
		)
	} else {
		return locker.Client.CreateTable(locker.TableName)
	}
}

// UseIn adds this locker to the passed composer.
func (locker *DynamoDBLocker) UseIn(composer *tusd.StoreComposer) {
	composer.UseLocker(locker)
}

// LockUpload tries to obtain the exclusive lock.
func (locker *DynamoDBLocker) LockUpload(id string) error {
	lock, err := locker.Client.AcquireLock(id,
		dynamolock.FailIfLocked(),
		dynamolock.WithDeleteLockOnRelease(),
	)
	if err != nil {
		log.Printf("Error locking file id: %s - error: %s", id, err.Error())
		return tusd.ErrFileLocked
	}

	locker.mutex.Lock()
	defer locker.mutex.Unlock()
	// Only add the lock to our list if the acquire was successful and no error appeared.
	locker.locks[id] = lock

	return nil
}

// UnlockUpload releases a lock.
func (locker *DynamoDBLocker) UnlockUpload(id string) error {
	locker.mutex.Lock()
	defer locker.mutex.Unlock()

	// Complain if no lock has been found. This can only happen if LockUpload
	// has not been invoked before or UnlockUpload multiple times.
	lock, ok := locker.locks[id]
	if !ok {
		return ErrLockNotHeld
	}

	success, err := locker.Client.ReleaseLock(lock, dynamolock.WithDeleteLock(true))
	if err != nil {
		return err
	}

	defer delete(locker.locks, id)
	// if success == false, then someone else already stole the lock
	if !success {
		return ErrLockNotHeld
	}
	return nil
}

// Close releases all the locks
func (locker *DynamoDBLocker) Close() {
	locker.mutex.Lock()
	defer locker.mutex.Unlock()
	for _, lock := range locker.locks {
		locker.Client.ReleaseLock(lock, dynamolock.WithDeleteLock(true))
	}
	locker.locks = map[string]*dynamolock.Lock{}
	locker.Client.Close()
}
