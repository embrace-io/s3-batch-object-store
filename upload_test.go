package s3batchstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	mocks3 "github.com/embrace-io/s3-batch-object-store/mock/aws"
	"github.com/klauspost/compress/zstd"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
)

func TestClient_UploadFile(t *testing.T) {
	g := NewGomegaWithT(t)
	ctx := context.Background()

	objs := []*TestObject{
		{
			ID:    "1",
			Value: "my first payload",
		},
		{
			ID:    "3",
			Value: "my third payload",
		},
		{
			ID:    "6",
			Value: "my sixth payload",
		},
	}
	compressedObjLengths := make([]int, len(objs))
	for i, obj := range objs {
		compressed, err := marshalAndCompress(obj)
		g.Expect(err).ToNot(HaveOccurred())
		compressedObjLengths[i] = len(compressed)
	}

	tests := []struct {
		name           string
		objs           []*TestObject
		withMetaFile   bool
		configureMocks func(g *WithT, file *TempFile[string], s3Mock *mocks3.MockS3Client)
		err            interface{}
	}{
		{
			name:         "successful upload with meta file",
			objs:         objs,
			withMetaFile: true,
			configureMocks: func(g *WithT, file *TempFile[string], s3Mock *mocks3.MockS3Client) {
				s3Mock.EXPECT().PutObject(ctx, matchUploadParams(file.fileName)).DoAndReturn(func(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					g.Expect(*input.Bucket).To(Equal(testBucketName))
					g.Expect(*input.Key).To(Equal(file.fileName))
					g.Expect(input.Body).ToNot(BeNil())
					g.Expect(input.Tagging).To(Equal(aws.String("retention-days=14")))
					return &s3.PutObjectOutput{}, nil
				})
				s3Mock.EXPECT().PutObject(ctx, matchUploadParams(file.MetaFileKey())).DoAndReturn(func(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					g.Expect(*input.Bucket).To(Equal(testBucketName))
					g.Expect(*input.Key).To(Equal(file.MetaFileKey()))
					g.Expect(input.Body).ToNot(BeNil())
					g.Expect(input.Tagging).To(Equal(aws.String("retention-days=14")))

					compressedBody, err := io.ReadAll(input.Body)
					g.Expect(err).ToNot(HaveOccurred())

					// Decompress the zstd-compressed body
					zstdReader, err := zstd.NewReader(nil)
					g.Expect(err).ToNot(HaveOccurred())
					body, err := zstdReader.DecodeAll(compressedBody, nil)
					g.Expect(err).ToNot(HaveOccurred())

					g.Expect(body).To(MatchJSON(`{` +
						`"1":{"file":"` + file.fileName + `","offset":0,"length":` + strconv.Itoa(compressedObjLengths[0]) + `},` +
						`"3":{"file":"` + file.fileName + `","offset":` + strconv.Itoa(compressedObjLengths[0]) + `,"length":` + strconv.Itoa(compressedObjLengths[1]) + `},` +
						`"6":{"file":"` + file.fileName + `","offset":` + strconv.Itoa(compressedObjLengths[0]+compressedObjLengths[1]) + `,"length":` + strconv.Itoa(compressedObjLengths[2]) + `}` +
						`}`))
					return &s3.PutObjectOutput{}, nil
				})
			},
		},
		{
			name:         "successful upload without meta file",
			objs:         objs,
			withMetaFile: false,
			configureMocks: func(g *WithT, file *TempFile[string], s3Mock *mocks3.MockS3Client) {
				// Only regular file expected
				s3Mock.EXPECT().PutObject(ctx, matchUploadParams(file.fileName)).DoAndReturn(func(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					g.Expect(*input.Bucket).To(Equal(testBucketName))
					g.Expect(*input.Key).To(Equal(file.fileName))
					g.Expect(input.Body).ToNot(BeNil())
					g.Expect(input.Tagging).To(Equal(aws.String("retention-days=14")))
					return &s3.PutObjectOutput{}, nil
				})
			},
		},
		{
			name:         "file readOnly error",
			objs:         objs,
			withMetaFile: true,
			configureMocks: func(g *WithT, file *TempFile[string], s3Mock *mocks3.MockS3Client) {
				// If for any reason the underlying file gets closed, we won't be able to get the readOnly contents.
				g.Expect(file.file.Close()).ToNot(HaveOccurred())
			},
			err: ContainSubstring("failed to get the readonly file: seek "),
		},
		{
			name:         "s3 upload error",
			objs:         objs,
			withMetaFile: true,
			configureMocks: func(g *WithT, file *TempFile[string], s3Mock *mocks3.MockS3Client) {
				s3Mock.EXPECT().PutObject(ctx, matchUploadParams(file.fileName)).DoAndReturn(func(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					g.Expect(*input.Bucket).To(Equal(testBucketName))
					g.Expect(*input.Key).To(Equal(file.fileName))
					g.Expect(input.Body).ToNot(BeNil())
					g.Expect(input.Tagging).To(Equal(aws.String("retention-days=14")))
					return nil, fmt.Errorf("s3 service error")
				})
			},
			err: "failed to upload data file to s3: s3 service error",
		},
		{
			name:         "s3 meta file upload error",
			objs:         objs,
			withMetaFile: true,
			configureMocks: func(g *WithT, file *TempFile[string], s3Mock *mocks3.MockS3Client) {
				s3Mock.EXPECT().PutObject(ctx, matchUploadParams(file.fileName)).DoAndReturn(func(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					g.Expect(*input.Bucket).To(Equal(testBucketName))
					g.Expect(*input.Key).To(Equal(file.fileName))
					g.Expect(input.Body).ToNot(BeNil())
					g.Expect(input.Tagging).To(Equal(aws.String("retention-days=14")))
					return &s3.PutObjectOutput{}, nil
				})
				s3Mock.EXPECT().PutObject(ctx, matchUploadParams(file.MetaFileKey())).DoAndReturn(func(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					g.Expect(*input.Bucket).To(Equal(testBucketName))
					g.Expect(*input.Key).To(Equal(file.MetaFileKey()))
					g.Expect(input.Body).ToNot(BeNil())
					g.Expect(input.Tagging).To(Equal(aws.String("retention-days=14")))

					compressedBody, err := io.ReadAll(input.Body)
					g.Expect(err).ToNot(HaveOccurred())

					// Decompress the zstd-compressed body
					zstdReader, err := zstd.NewReader(nil)
					g.Expect(err).ToNot(HaveOccurred())
					body, err := zstdReader.DecodeAll(compressedBody, nil)
					g.Expect(err).ToNot(HaveOccurred())

					g.Expect(body).To(MatchJSON(`{` +
						`"1":{"file":"` + file.fileName + `","offset":0,"length":` + strconv.Itoa(compressedObjLengths[0]) + `},` +
						`"3":{"file":"` + file.fileName + `","offset":` + strconv.Itoa(compressedObjLengths[0]) + `,"length":` + strconv.Itoa(compressedObjLengths[1]) + `},` +
						`"6":{"file":"` + file.fileName + `","offset":` + strconv.Itoa(compressedObjLengths[0]+compressedObjLengths[1]) + `,"length":` + strconv.Itoa(compressedObjLengths[2]) + `}` +
						`}`))
					return nil, fmt.Errorf("s3 service error")
				})
			},
			err: "failed to upload meta file to s3: s3 service error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := NewGomegaWithT(t)

			ctrl := gomock.NewController(t)
			s3Mock := mocks3.NewMockS3Client(ctrl)

			c := &client[string]{
				s3Bucket: testBucketName,
				s3Client: s3Mock,
			}

			file, err := c.NewTempFile(testTags)
			g.Expect(err).ToNot(HaveOccurred())
			defer func() { _ = file.Close() }()

			for _, objs := range test.objs {
				compressed, err := marshalAndCompress(objs)
				g.Expect(err).ToNot(HaveOccurred())
				_, err = file.AppendAndReturnIndex(objs.ID, compressed)
				g.Expect(err).ToNot(HaveOccurred())
			}
			g.Expect(file.Count()).To(Equal(uint(len(test.objs))))
			g.Expect(file.Age()).To(BeNumerically(">=", uint64(0)))
			g.Expect(file.Size()).To(BeNumerically(">=", uint64(0)))

			test.configureMocks(g, file, s3Mock)
			err = c.UploadFile(ctx, file, test.withMetaFile)
			if test.err == nil {
				g.Expect(err).ToNot(HaveOccurred())
			} else {
				g.Expect(err).To(MatchError(test.err))
			}
		})
	}
}

func TestClient_DeleteFile(t *testing.T) {
	g := NewGomegaWithT(t)

	ctx := context.Background()
	objs := []*TestObject{
		{
			ID:    "1",
			Value: "my first payload",
		},
		{
			ID:    "2",
			Value: "my second payload",
		},
		{
			ID:    "3",
			Value: "my third payload",
		},
	}
	compressedObjLengths := make([]int, len(objs))
	for i, obj := range objs {
		compressed, err := marshalAndCompress(obj)
		g.Expect(err).ToNot(HaveOccurred())
		compressedObjLengths[i] = len(compressed)
	}

	tests := []struct {
		name           string
		objs           []*TestObject
		configureMocks func(g *WithT, ctrl *gomock.Controller, file *TempFile[string], s3Mock *mocks3.MockS3Client)
		err            interface{}
	}{
		{
			name: "successful delete",
			objs: objs,
			configureMocks: func(g *WithT, ctrl *gomock.Controller, file *TempFile[string], s3Mock *mocks3.MockS3Client) {
				// One delete call with the 2 files: regular file and meta file
				metaFileKey := file.MetaFileKey()
				s3Mock.EXPECT().DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: aws.String(testBucketName),
					Delete: &types.Delete{
						Objects: []types.ObjectIdentifier{
							{Key: &file.fileName},
							{Key: &metaFileKey},
						},
					},
				}).Return(&s3.DeleteObjectsOutput{}, nil).Times(1)
			},
		},
		{
			name: "error deleting s3 files",
			objs: objs,
			configureMocks: func(g *WithT, ctrl *gomock.Controller, file *TempFile[string], s3Mock *mocks3.MockS3Client) {
				metaFileKey := file.MetaFileKey()
				s3Mock.EXPECT().DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: aws.String(testBucketName),
					Delete: &types.Delete{
						Objects: []types.ObjectIdentifier{
							{Key: &file.fileName},
							{Key: &metaFileKey},
						},
					},
				}).Return(nil, errors.New("error deleting s3 file")).Times(1)
			},
			err: "failed to delete files: error deleting s3 file",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := NewGomegaWithT(t)

			ctrl := gomock.NewController(t)
			s3Mock := mocks3.NewMockS3Client(ctrl)

			c := &client[string]{
				s3Bucket: testBucketName,
				s3Client: s3Mock,
			}

			file, err := c.NewTempFile(testTags)
			g.Expect(err).ToNot(HaveOccurred())
			defer func() { _ = file.Close() }()

			for _, obj := range test.objs {
				compressed, err := marshalAndCompress(obj)
				g.Expect(err).ToNot(HaveOccurred())
				_, err = file.AppendAndReturnIndex(obj.ID, compressed)
				g.Expect(err).ToNot(HaveOccurred())
			}
			g.Expect(file.Count()).To(Equal(uint(len(test.objs))))
			g.Expect(file.Age()).To(BeNumerically(">=", uint64(0)))
			g.Expect(file.Size()).To(BeNumerically(">=", uint64(0)))

			test.configureMocks(g, ctrl, file, s3Mock)
			err = c.DeleteFile(ctx, file)
			if test.err == nil {
				g.Expect(err).ToNot(HaveOccurred())
			} else {
				g.Expect(err).To(MatchError(test.err))
			}
		})
	}
}

type uploadParamsMatcher struct {
	fileKey string
}

func matchUploadParams(fileKey string) gomock.Matcher {
	return &uploadParamsMatcher{fileKey: fileKey}
}

func (matcher *uploadParamsMatcher) Matches(actual interface{}) bool {
	actualInput, actualOk := actual.(*s3.PutObjectInput)
	return actualOk && *actualInput.Key == matcher.fileKey
}

func (matcher *uploadParamsMatcher) String() string {
	return fmt.Sprintf("uploader with key: %s", matcher.fileKey)
}
