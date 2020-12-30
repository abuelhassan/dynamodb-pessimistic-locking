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
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const (
	tableName = "pessimistic-locking"

	// Table keys
	pk = "PK"
	sk = "SK"

	// Config
	maxRetries  = 5
	readTimeout = 1 * time.Second
)

var (
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

	sess := session.Must(session.NewSession())
	r := &retryer{
		def: client.DefaultRetryer{
			NumMaxRetries: maxRetries,
		},
	}
	svc := dynamodb.New(sess, request.WithRetryer(aws.NewConfig(), r))

	_, err := svc.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			pk: {S: aws.String(inp.PK)},
			sk: {S: aws.String(fmt.Sprintf("#%s", inp.PK))},
		},
		ConditionExpression: aws.String("wlock = :fls OR wtime < :nw"),
		UpdateExpression:    aws.String("set readers = readers + :one, rtime = :rt"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":one": {N: aws.String("1")},
			":fls": {BOOL: aws.Bool(false)},
			":nw":  {N: aws.String(getTimeStamp(time.Now()))},
			":rt":  {N: aws.String(getTimeStamp(time.Now().Add(readTimeout)))},
		},
	})
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

	defer func() {
		_, err = svc.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(tableName),
			Key: map[string]*dynamodb.AttributeValue{
				pk: {S: aws.String(inp.PK)},
				sk: {S: aws.String(fmt.Sprintf("#%s", inp.PK))},
			},
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

	result, err := svc.QueryWithContext(ctx, &dynamodb.QueryInput{
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

func main() {
	lambda.Start(handler)
}

// TODO: move to layer

type retryer struct {
	def client.DefaultRetryer
}

func (r retryer) RetryRules(req *request.Request) time.Duration {
	return r.def.RetryRules(req)
}

func (r retryer) ShouldRetry(req *request.Request) bool {
	if aerr, ok := req.Error.(awserr.Error); ok {
		if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return true
		}
	}

	return r.def.ShouldRetry(req)
}

func (r retryer) MaxRetries() int {
	return r.def.MaxRetries()
}

func getTimeStamp(t time.Time) string {
	return strconv.FormatInt(t.UTC().Unix(), 10)
}
