package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"abuelhassan/dynamodb-pessimistic-locking/helpers"
)

const (
	tableName = "pessimistic-locking"

	// Table keys
	pk = "PK"
	sk = "SK"

	// Config
	readTimeout = 1 * time.Second
)

var (
	dbClient *dynamodb.DynamoDB

	errInvalid = errors.New("invalid")
	errBlocked = errors.New("blocked")
	errUnknown = errors.New("unknown")
)

type (
	input struct {
		PK string `json:"PK"`
	}
	output struct {
		PK          string `json:"PK"`
		SK          string `json:"SK"`
		Description string `json:"desc"`
	}
)

func handler(ctx context.Context, inp input) ([]output, error) {
	if inp.PK == "" {
		return nil, errInvalid
	}

	key := map[string]*dynamodb.AttributeValue{
		pk: {S: aws.String(inp.PK)},
		sk: {S: aws.String(fmt.Sprintf("#%s", inp.PK))},
	}

	// check if a write operation is blocking, and increment read blockers.
	// (wtime < :nw) is only considered for the original request, and not the retried requests.
	_, err := dbClient.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(tableName),
		Key:                 key,
		ConditionExpression: aws.String("wlock = :fls OR wtime < :nw"),
		UpdateExpression:    aws.String("set readers = readers + :one, rtime = :rt"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":one": {N: aws.String("1")},
			":fls": {BOOL: aws.Bool(false)},
			":nw":  {N: aws.String(getTimeStamp(time.Now()))},
			":rt":  {N: aws.String(getTimeStamp(time.Now().Add(readTimeout)))},
		},
	}, helpers.ConditionFailedRetryOption)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				fmt.Println(aerr.Message())
				return nil, errBlocked
			}
		}
		fmt.Println(err)
		return nil, errUnknown
	}

	// release lock.
	defer func() {
		_, err := dbClient.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
			TableName:           aws.String(tableName),
			Key:                 key,
			ConditionExpression: aws.String("readers <> :zero"),
			UpdateExpression:    aws.String("set readers = readers - :one"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":zero": {N: aws.String("0")},
				":one":  {N: aws.String("1")},
			},
		})
		if err != nil {
			fmt.Println(fmt.Errorf("undo lock: %w", err))
		}
	}()

	result, err := dbClient.QueryWithContext(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk", pk)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(inp.PK)},
		},
	})
	if err != nil {
		fmt.Println(err)
		return nil, errUnknown
	}

	var out []output
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &out)
	if err != nil {
		fmt.Println(err)
		return nil, errUnknown
	}
	return out, nil
}

func getTimeStamp(t time.Time) string {
	return strconv.FormatInt(t.UTC().Unix(), 10)
}

func main() {
	lambda.Start(handler)
}

func init() {
	sess := session.Must(session.NewSession())
	dbClient = dynamodb.New(sess)
}
