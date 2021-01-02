package helpers

// TODO: deploy this package on a layer to reduce the size of uploaded archives.

import (
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	// change based on time consumed by queries.
	// this waits for 1sec.
	maxRetries = 5
	delay      = 200 * time.Millisecond
)

func ConditionFailedRetryOption(req *request.Request) {
	req.RetryErrorCodes = append(req.RetryErrorCodes, dynamodb.ErrCodeConditionalCheckFailedException)
	req.Retryer = retryer{
		DefaultRetryer: client.DefaultRetryer{
			NumMaxRetries: maxRetries,
		},
	}
}

// retryer implements request.Retryer. It has the same behavior of a DefaultRetryer,
// except that it retries errors of type dynamodb.ErrCodeConditionalCheckFailedException.
type retryer struct {
	client.DefaultRetryer
}

func (r retryer) RetryRules(req *request.Request) time.Duration {
	if aerr, ok := req.Error.(awserr.Error); ok {
		if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return delay
		}
	}
	return r.DefaultRetryer.RetryRules(req)
}
