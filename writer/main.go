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
)

const (
	tableName = "pessimistic-locking"

	// Table keys
	pk   = "PK"
	sk   = "SK"
	desc = "desc"

	// Config
	maxRetries   = 5
	writeTimeout = 5 * time.Second
)

var (
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
)

func handler(ctx context.Context, inp input) error {
	if inp.PK == "" {
		return errInvalid
	}
	if len(inp.Chores) == 0 {
		return nil
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
		ConditionExpression: aws.String("(readers = :zero OR rtime < :nw) AND (wlock = :fls OR wtime < :nw)"),
		UpdateExpression:    aws.String("set readers = :zero, wtime = :wt, wlock = :tru"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":zero": {N: aws.String("0")},
			":fls":  {BOOL: aws.Bool(false)},
			":tru":  {BOOL: aws.Bool(true)},
			":nw":   {N: aws.String(getTimeStamp(time.Now()))},
			":wt":   {N: aws.String(getTimeStamp(time.Now().Add(writeTimeout)))},
		},
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return errBlocked
			}
		}
		fmt.Println(err)
		return errUnknown
	}

	defer func() {
		_, err = svc.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(tableName),
			Key: map[string]*dynamodb.AttributeValue{
				pk: {S: aws.String(inp.PK)},
				sk: {S: aws.String(fmt.Sprintf("#%s", inp.PK))}, // SK for the metadata item is the same as PK with a `#` as a prefix.
			},
			ConditionExpression: aws.String("wlock = :tru"),
			UpdateExpression:    aws.String("set wlock = :fls"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":fls": {BOOL: aws.Bool(false)},
				":tru": {BOOL: aws.Bool(true)},
			},
		})
		if err != nil {
			fmt.Println(fmt.Errorf("undo lock: %w", err))
		}
	}()

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
					desc: {
						S: aws.String(chore.Description),
					},
				},
			},
		}
	}
	_, err = svc.BatchWriteItemWithContext(ctx, &req)
	if err != nil {
		fmt.Println(err)
		return errUnknown
	}

	return nil
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
