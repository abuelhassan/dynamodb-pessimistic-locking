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
	writeTimeout = 5 * time.Second
)

var (
	dbClient *dynamodb.DynamoDB

	errInvalid = errors.New("invalid")
	errBlocked = errors.New("blocked")
	errUnknown = errors.New("unknown")
)

type (
	input struct {
		PK     string  `json:"PK"`
		Chores []Chore `json:"chores"`
	}
	Chore struct {
		Name        string `json:"name"`
		Description string `json:"desc"`
	}
	metadata struct {
		PK      string `json:"PK"`
		SK      string `json:"SK"`
		Readers int    `json:"readers"`
		RTime   int64  `json:"rtime"`
	}
)

func handler(ctx context.Context, inp input) error {
	if inp.PK == "" {
		return errInvalid
	}
	if len(inp.Chores) == 0 {
		return nil
	}

	key := map[string]*dynamodb.AttributeValue{
		pk: {S: aws.String(inp.PK)},
		sk: {S: aws.String(fmt.Sprintf("#%s", inp.PK))},
	}

	// lock both reading and writing if there are no active writers.
	// (wtime < :nw) is only considered for the original request, and not the retried requests.
	res, err := dbClient.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(tableName),
		Key:                 key,
		ReturnValues:        aws.String(dynamodb.ReturnValueAllNew),
		ConditionExpression: aws.String("wlock = :fls OR wtime < :nw"),
		UpdateExpression:    aws.String("set wlock = :tru, wtime = :wt"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":fls": {BOOL: aws.Bool(false)},
			":tru": {BOOL: aws.Bool(true)},
			":nw":  {N: aws.String(getTimeStamp(time.Now()))},
			":wt":  {N: aws.String(getTimeStamp(time.Now().Add(writeTimeout)))},
		},
	}, helpers.ConditionFailedRetryOption)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return errBlocked
			}
		}
		fmt.Println(err)
		return errUnknown
	}

	// release lock.
	defer func() {
		_, err := dbClient.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
			TableName:        aws.String(tableName),
			Key:              key,
			UpdateExpression: aws.String("set wlock = :fls"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":fls": {BOOL: aws.Bool(false)},
			},
		})
		if err != nil {
			fmt.Println(fmt.Errorf("undo lock: %w", err))
		}
	}()

	m := metadata{}
	err = dynamodbattribute.UnmarshalMap(res.Attributes, &m)
	if err != nil {
		fmt.Println(err)
		return errUnknown
	}
	if m.Readers != 0 {
		// wait for all readers to release their locks, and update wlock timeout.
		// (rtime < :nw) is only considered for the original request, and not the retried requests.
		_, err = dbClient.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
			TableName:           aws.String(tableName),
			Key:                 key,
			ConditionExpression: aws.String("readers = :zero OR rtime < :nw"),
			UpdateExpression:    aws.String("set readers = :zero, wtime = :wt"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":zero": {N: aws.String("0")},
				":nw":   {N: aws.String(getTimeStamp(time.Now()))},
				":wt":   {N: aws.String(getTimeStamp(time.Now().Add(writeTimeout)))},
			},
		}, helpers.ConditionFailedRetryOption)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
					return errBlocked
				}
			}
			// instead of failing, we can wait until we make sure all the readers are expired.
			fmt.Println(err)
			return errUnknown
		}
	}

	// update item.
	req := dynamodb.BatchWriteItemInput{}
	req.RequestItems = map[string][]*dynamodb.WriteRequest{
		tableName: make([]*dynamodb.WriteRequest, len(inp.Chores)),
	}
	for i, chore := range inp.Chores {
		req.RequestItems[tableName][i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					pk: {
						S: aws.String(inp.PK),
					},
					sk: {
						S: aws.String(fmt.Sprintf("CHORE#%s", chore.Name)),
					},
					"desc": {
						S: aws.String(chore.Description),
					},
				},
			},
		}
	}
	_, err = dbClient.BatchWriteItemWithContext(ctx, &req)
	if err != nil {
		fmt.Println(err)
		return errUnknown
	}

	return nil
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
