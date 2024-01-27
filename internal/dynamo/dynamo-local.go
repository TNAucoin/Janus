package dynamo

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconf "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"time"
)

// LocalTableInput is a variable that represents the input parameters for creating a local DynamoDB table.
// It is a function that takes a tableName string parameter and returns a pointer to dynamodb.CreateTableInput.
// Make sure to have a valid DDBConnection object with a valid Client, Region, and TableName before using LocalTableInput.
var LocalTableInput = func(tableName string, indexName string) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("last_updated_timestamp"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("queued"),
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(indexName),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("queued"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("last_updated_timestamp"),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					NonKeyAttributes: nil,
					ProjectionType:   types.ProjectionTypeAll,
				},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(10),
					WriteCapacityUnits: aws.Int64(10),
				},
			},
		},
		TableName: aws.String(tableName),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	}
}

// CreateLocalTable creates a local DynamoDB table if it doesn't already exist.
func CreateLocalTable(ddbc *DDBConnection) error {
	exists, err := checkIfLocalTableExists(ddbc)
	if err != nil {
		return err
	}
	if exists {
		log.Printf("table: %s already exists, skipping creation", ddbc.TableName)
	} else {
		log.Printf("table not found creating a new local table...")
		err := createDynamoDBTable(ddbc)
		if err != nil {
			return err
		}
	}
	return nil
}

// createDynamoDBTable creates a DynamoDB table using the provided DDBConnection.
// It takes the DDBConnection as a parameter and returns an error if the table creation fails.
func createDynamoDBTable(ddbc *DDBConnection) error {
	_, err := ddbc.Client.CreateTable(context.TODO(), LocalTableInput(ddbc.TableName, ddbc.IndexName))
	if err != nil {
		log.Printf("%v", err)
		return errors.New("failed to create table")
	}
	waiter := dynamodb.NewTableExistsWaiter(ddbc.Client)
	err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(ddbc.TableName),
	}, 5*time.Minute)
	if err != nil {
		log.Printf("%v", err)
		return errors.New("table creation timed out")
	}
	log.Printf("successfully created the table: %s", ddbc.TableName)
	return nil
}

// createDynamoDbURL creates a DynamoDB URL with the given hostURL and port.
// The hostURL specifies the address of the DynamoDB service.
// The port specifies the port number to be used for the connection.
// If the specified port is not within the range of 0 to 65535, an error is returned.
// The function returns the created DynamoDB URL or an error if the port is invalid.
func createDynamoDbURL(hostURL string, port int) (string, error) {
	if port < 0 || port > 65535 {
		return "", errors.New("invalid port range")
	}
	url := fmt.Sprintf("http://%s:%d", hostURL, port)
	return url, nil
}

// createLocalConfig creates an AWS Config object with the specified region and URL.
// It uses hard-coded credentials for local usage.
// This function is typically used within the New function of DDBConnection to create a connection to DynamoDB.
// Parameters:
// - region: The AWS region.
// - url: The URL to connect to DynamoDB.
// Returns:
// - aws.Config: The AWS Config object.
// - error: Any error that occurred during configuration loading.
func createLocalConfig(region, url string) (aws.Config, error) {
	cfg, err := awsconf.LoadDefaultConfig(context.Background(),
		awsconf.WithRegion(region),
		awsconf.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: url}, nil
			},
		)),
		awsconf.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "dummy",
				SecretAccessKey: "dummy",
				SessionToken:    "dummy",
				Source:          "Hard-coded credentials; useless local values",
			},
		}),
	)
	if err != nil {
		return aws.Config{}, err
	}
	return cfg, nil
}

// checkIfLocalTableExists checks if a local DynamoDB table exists.
// It takes a DDBConnection object as parameter which contains the DynamoDB client, region, and table name.
// It returns a boolean value indicating if the table exists and an error if any.
// It first calls the DescribeTable API to check if the table exists.
// If the table is not found, it sets the exists variable to false.
// If any error occurs during the API call, it returns the error.
// Finally, it returns the exists value and nil error if successful.
func checkIfLocalTableExists(ddbc *DDBConnection) (bool, error) {
	exists := true
	_, err := ddbc.Client.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(ddbc.TableName),
	})
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			exists = false
		} else {
			return exists, err
		}
	}
	return exists, nil
}
