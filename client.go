package s3batchstore

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Client is the object S3 client used to store and fetch object to/from s3, by using the index information
type Client interface {
	// UploadToS3 will take a TempFile that already has all the objects in it, and upload it to a s3 file,
	// in one single operation.
	// withMetaFile indicates whether the metadata will be also uploaded to the file.MetaFileKey() location,
	// with the index information for each object, or not.
	UploadToS3(ctx context.Context, file *TempFile, withMetaFile bool) error

	// DeleteFromS3 allows to try to delete any files that may have been uploaded to s3 based on the provided file.
	// This is provided in case of any error when calling UploadToS3, callers have the possibility to clean up the files.
	DeleteFromS3(ctx context.Context, file *TempFile) error

	// Fetch downloads the payload from s3 given the ObjectIndex, fetching only the needed bytes, and returning
	// the payload as a byte array.
	// The caller is responsible for decompressing/unmarshalling or any operation needed to parse it to the proper struct.
	Fetch(ctx context.Context, ind ObjectIndex) ([]byte, error)
}

// S3Client is used to mock the aws s3 functions used in this module.
//
//go:generate mockgen -destination=./mock/mock_s3client.go -package=mocks3 . S3Client
type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type client struct {
	s3Client S3Client
	s3Bucket string
}

func NewClient(awsConfig aws.Config, s3Bucket string) Client {
	s3Client := s3.NewFromConfig(awsConfig)
	return &client{
		s3Client: s3Client,
		s3Bucket: s3Bucket,
	}
}
