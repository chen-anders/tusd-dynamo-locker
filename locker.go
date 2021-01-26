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
	tusd "github.com/tus/tusd/pkg/handler"
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

func (locker *DynamoDBLocker) NewLock(id string) (tusd.Lock, error) {
	return &Lock{
		locker,
		id,
	}, nil
}

type Lock struct {
	locker *DynamoDBLocker
	id string
}

// Lock tries to obtain the exclusive lock.
func (lock Lock) Lock() error {
	refreshPeriod := time.Duration(lock.locker.LeaseDuration / 10) * time.Millisecond
	acquiredLock, err := lock.locker.Client.AcquireLock(lock.id,
		dynamolock.WithRefreshPeriod(refreshPeriod),
		dynamolock.WithDeleteLockOnRelease(),
	)
	if err != nil {
		log.Printf("Error locking file id: %s - error: %s", lock.id, err.Error())
		return tusd.ErrFileLocked
	}

	lock.locker.mutex.Lock()
	defer lock.locker.mutex.Unlock()
	// Only add the acquiredLock to our list if the acquire was successful and no error appeared.
	lock.locker.locks[lock.id] = acquiredLock

	return nil
}

// Unlock releases the lock.
func (lock Lock) Unlock() error {
	lock.locker.mutex.Lock()
	defer lock.locker.mutex.Unlock()

	// Complain if no lock has been found. This can only happen if Lock()
	// has not been invoked before or Unlock() multiple times.
	acquiredLock, ok := lock.locker.locks[lock.id]
	if !ok {
		return ErrLockNotHeld
	}

	success, err := lock.locker.Client.ReleaseLock(acquiredLock, dynamolock.WithDeleteLock(true))
	if err != nil {
		return err
	}

	defer delete(lock.locker.locks, lock.id)
	// if success == false, then someone else already stole the lock
	if !success {
		return ErrLockNotHeld
	}
	return nil
}
