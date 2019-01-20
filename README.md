# tusd-dynamo-locker

[![Build Status](https://travis-ci.com/chen-anders/tusd-dynamo-locker.svg?branch=master)](https://travis-ci.com/chen-anders/tusd-dynamo-locker)

[Godoc](https://godoc.org/github.com/chen-anders/tusd-dynamo-locker)

This package provides a DynamoDB-based locker for [tusd](https://github.com/tus/tusd) - a reference implementation of a server that allows resumable uploads via the tus protocol.

### Usage

#### Creating a New DynamoDB Table

A helpful CLI tool has been created to allow one to easily provision a DynamoDB table that can be used with this locker. This tool assumes that the correct AWS credentials have been set or a session (with proper IAM permissions) is currently active.

```
go build -o setup cmd/setup/main.go
```

```
Usage of ./setup:
  -read-capacity-units int
      DynamoDB Read Capacity Units for Provisioned Capacity
  -region string
      AWS Region (default "us-east-1")
  -table-name string
      DynamoDB Table Name
  -write-capacity-units int
      DynamoDB Write Capacity Units for Provisioned Capacity
```


#### In your tusd server

To initialize a locker, a pre-existing connected DynamoDB client must be present.
```
import (
  dynamolocker "github.com/chen-anders/tusd-dynamo-locker"
)

...

sess := session.Must(session.NewSession(&aws.Config{
  Region: aws.String("us-east-1"),
}))

dynamoDBClient := dynamodb.New(sess)
locker, err := dynamolocker.New(dyanmoDBClient, "my-locker")
if err != nil {
  log.Fatal(err)
}
```

The locker will need to be included in composer that is used by tusd
```
composer := tusd.NewStoreComposer()
locker.UseIn(composer)
```

