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
	delay      = 100 * time.Millisecond
)

func ConditionFailedRetryOption(req *request.Request) {
	req.RetryErrorCodes = append(req.RetryErrorCodes, dynamodb.ErrCodeConditionalCheckFailedException)
	req.Retryer = retryer{
		DefaultRetryer: client.DefaultRetryer{
			NumMaxRetries: maxRetries,
		},
	}
}

// retryer implements aws's request.Retryer.
// It retries errors of type dynamodb.ErrCodeConditionalCheckFailedException with a configured delay time.
// For other types of errors, it has the same behavior of the default retryer (exponential backoff and jitter).
type retryer struct {
	client.DefaultRetryer
}

// no need to implement this function in case you want to depend on 
// the exponential backoff and jitter algorithm with the default values for failed conditions.
func (r retryer) RetryRules(req *request.Request) time.Duration {
	if aerr, ok := req.Error.(awserr.Error); ok {
		if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return delay
		}
	}
	return r.DefaultRetryer.RetryRules(req)
}
