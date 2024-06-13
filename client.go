package s3batchstore

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Client is the object S3 client used to store and fetch object to/from s3, by using the index information
//
//go:generate mockgen -source=./client.go -destination=./mock/client/mock_client.go -package=mocks3batchstore Client
type Client[K comparable] interface {
	// NewTempFile Creates a new file in a temp folder
	// tags can be used to store information about this file in S3, like retention days
	// The file itself is not thread safe, if you expect to make concurrent calls to Append, you should protect it.
	// Once all the objects are appended, you can call UploadToS3 to upload the file to s3.
	NewTempFile(tags map[string]string) (*TempFile[K], error)

	// UploadToS3 will take a TempFile that already has all the objects in it, and upload it to a s3 file,
	// in one single operation.
	// withMetaFile indicates whether the metadata will be also uploaded to the file.MetaFileKey() location,
	// with the index information for each object, or not.
	UploadToS3(ctx context.Context, file *TempFile[K], withMetaFile bool) error

	// DeleteFromS3 allows to try to delete any files that may have been uploaded to s3 based on the provided file.
	// This is provided in case of any error when calling UploadToS3, callers have the possibility to clean up the files.
	DeleteFromS3(ctx context.Context, file *TempFile[K]) error

	// Fetch downloads the payload from s3 given the ObjectIndex, fetching only the needed bytes, and returning
	// the payload as a byte array.
	// The caller is responsible for decompressing/unmarshalling or any operation needed to parse it to the proper struct.
	Fetch(ctx context.Context, ind ObjectIndex) ([]byte, error)
}

// S3Client is used to mock the aws s3 functions used in this module.
//
//go:generate mockgen -destination=./mock/aws/mock_s3client.go -package=mocks3 . S3Client
type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type client[K comparable] struct {
	s3Client S3Client
	s3Bucket string
}

// NewClient creates a new client that can be used to upload and download objects to s3.
// K represents the type of IDs for the objects that will be uploaded and fetched.
func NewClient[K comparable](awsConfig aws.Config, s3Bucket string) Client[K] {
	s3Client := s3.NewFromConfig(awsConfig)
	return &client[K]{
		s3Client: s3Client,
		s3Bucket: s3Bucket,
	}
}
