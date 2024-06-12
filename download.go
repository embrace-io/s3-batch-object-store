package s3batchstore

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (c *client[K]) Fetch(ctx context.Context, ind ObjectIndex) ([]byte, error) {
	byteRange := byteRangeString(ind.Offset, ind.Length)
	result, err := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.s3Bucket),
		Key:    aws.String(ind.File),
		Range:  aws.String(byteRange),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download object from file %s/%s %s: %w", c.s3Bucket, ind.File, byteRange, err)
	}

	defer func() { _ = result.Body.Close() }()
	return io.ReadAll(result.Body)
}

// byteRangeString generates the byte range to read a byte range from an s3 file.
func byteRangeString(offset, length uint64) string {
	return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
}
