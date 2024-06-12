package s3batchstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (c *client[K]) UploadToS3(ctx context.Context, file *TempFile[K], withMetaFile bool) error {
	body, err := file.readOnly()
	if err != nil {
		return fmt.Errorf("failed to get the readonly file: %w", err)
	}

	tagging := serializeTags(file.Tags())
	_, err = c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  &c.s3Bucket,
		Key:     &file.fileName,
		Body:    body,
		Tagging: &tagging,
	})
	if err != nil {
		return fmt.Errorf("failed to upload data file to s3: %w", err)
	}

	if withMetaFile {
		// If requested, also upload the meta file:
		metafileKey := file.MetaFileKey()
		metafileBody, err := json.Marshal(file.indexes)
		if err != nil {
			return fmt.Errorf("failed to marshal meta body: %w", err)
		}
		_, err = c.s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:  &c.s3Bucket,
			Key:     &metafileKey,
			Body:    bytes.NewReader(metafileBody),
			Tagging: &tagging,
		})
		if err != nil {
			return fmt.Errorf("failed to upload meta file to s3: %w", err)
		}
	}

	return nil
}

func (c *client[K]) DeleteFromS3(ctx context.Context, file *TempFile[K]) error {
	metafileKey := file.MetaFileKey()
	_, err := c.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: &c.s3Bucket,
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: &file.fileName},
				{Key: &metafileKey},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to delete files: %w", err)
	}
	return nil
}

func serializeTags(tags map[string]string) string {
	params := url.Values{}
	for k, v := range tags {
		params.Add(k, v)
	}
	encoded := params.Encode()
	return encoded
}
