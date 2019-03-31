package main

import (
	"flag"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	dynamolocker "github.com/chen-anders/tusd-dynamo-locker"
)

func main() {
	tableName := flag.String("table-name", "", "Required. DynamoDB Table Name")
	region := flag.String("region", "us-east-1", "AWS Region")
	readCapacityUnits := flag.Int64("read-capacity-units", 0, "(optional) DynamoDB Read Capacity Units for Provisioned Capacity.")
	writeCapacityUnits := flag.Int64("write-capacity-units", 0, "(optional) DynamoDB Write Capacity Units for Provisioned Capacity.")
	flag.Parse()

	if *tableName == "" {
		log.Fatalln("Table name is required!")
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(*region),
	}))

	dynamoDBClient := dynamodb.New(sess)
	locker, err := dynamolocker.New(dynamoDBClient, *tableName)
	if err != nil {
		log.Fatalf("Error intializing locker: %v \n", err.Error())
	}
	tableOpts := dynamolocker.DynamoDBTableOptions{}

	if *readCapacityUnits == 0 && *writeCapacityUnits == 0 {
		// do nothing
	} else {
		if *readCapacityUnits == 0 || *writeCapacityUnits == 0 {
			log.Fatalln("For provisioned capacity, both read/write capacity units should be over 0.")
		}
		tableOpts.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(*readCapacityUnits),
			WriteCapacityUnits: aws.Int64(*writeCapacityUnits),
		}
	}

	_, err = locker.CreateDynamoDBTable(&tableOpts)
	if err != nil {
		log.Fatalf("Error creating DynamoDB Table: %v \n", err.Error())
	} else {
		log.Printf("DynamoDB Table (%s) is provisioning.", *tableName)
	}
}
