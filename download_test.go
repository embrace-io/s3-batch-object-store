package s3batchstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	mocks3 "github.com/embrace-io/s3-batch-object-store/mock"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
)

type objUploadFixture struct {
	objID             string
	payload           []byte
	obj               *TestObject
	compressedPayload []byte
	offset            uint64
	length            uint64
}

func newObjectUploadFixture(g *WithT, obj *TestObject) objUploadFixture {
	payload, err := json.Marshal(obj)
	g.Expect(err).ToNot(HaveOccurred())
	compressed, err := gzipCompress(payload)
	g.Expect(err).ToNot(HaveOccurred())
	return objUploadFixture{
		objID:             obj.ID,
		payload:           payload,
		obj:               obj,
		compressedPayload: compressed,
		length:            uint64(len(compressed)),
	}
}

func TestClient_Fetch(t *testing.T) {
	g := NewGomegaWithT(t)
	fixture1 := newObjectUploadFixture(g, &TestObject{ID: "1", Value: "my first payload"})
	fixture2 := newObjectUploadFixture(g, &TestObject{ID: "2", Value: "my second payload"})
	fixture3 := newObjectUploadFixture(g, &TestObject{ID: "3", Value: "my third payload"})

	fixture1.offset = 0
	fixture2.offset = fixture1.length
	fixture3.offset = fixture1.length + fixture2.length

	expectedIndexes := map[string]ObjectIndex{
		fixture1.objID: {Offset: fixture1.offset, Length: fixture1.length},
		fixture2.objID: {Offset: fixture2.offset, Length: fixture2.length},
		fixture3.objID: {Offset: fixture3.offset, Length: fixture3.length},
	}

	bytesByID := map[string][]byte{
		fixture1.objID: fixture1.payload,
		fixture2.objID: fixture2.payload,
		fixture3.objID: fixture3.payload,
	}

	ctrl := gomock.NewController(t)
	s3Mock := mocks3.NewMockS3Client(ctrl)

	c := client[string]{
		s3Bucket: testBucketName,
		s3Client: s3Mock,
	}

	file, err := c.NewTempFile(testTags)
	g.Expect(err).ToNot(HaveOccurred())
	defer func() { _ = file.Close() }()

	ctx := context.Background()

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
		return &s3.PutObjectOutput{}, nil
	})
	s3Mock.EXPECT().GetObject(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, _ ...func(options *s3.Options)) (*s3.GetObjectOutput, error) {
		payloadsByRange := map[string][]byte{
			byteRangeString(fixture1.offset, fixture1.length): fixture1.compressedPayload,
			byteRangeString(fixture2.offset, fixture2.length): fixture2.compressedPayload,
			byteRangeString(fixture3.offset, fixture3.length): fixture3.compressedPayload,
		}
		out, ok := payloadsByRange[*input.Range]
		g.Expect(ok).To(BeTrue(), fmt.Sprintf("input range %s is not a valid range", *input.Range))

		var buf bytes.Buffer
		_, err = buf.Write(out)
		g.Expect(err).ToNot(HaveOccurred())

		return &s3.GetObjectOutput{
			Body: io.NopCloser(&buf),
		}, nil
	}).Times(3)

	for _, fixture := range []objUploadFixture{fixture1, fixture2, fixture3} {
		b, err := marshalAndCompress(fixture.obj)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(file.Append(fixture.obj.ID, b)).ToNot(HaveOccurred())
	}

	err = c.UploadToS3(ctx, file, true)
	g.Expect(err).To(BeNil())
	g.Expect(len(file.indexes)).To(Equal(len(expectedIndexes)))
	for id, index := range expectedIndexes {
		idx, ok := file.indexes[id]
		g.Expect(ok).To(BeTrue())
		g.Expect(idx.Offset).To(Equal(index.Offset))
		g.Expect(idx.Length).To(Equal(index.Length))
	}

	for id, ind := range expectedIndexes {
		b, err := c.Fetch(ctx, ind)
		g.Expect(err).To(BeNil())

		actualPayload, err := gzipDecompress(b)
		g.Expect(err).To(BeNil())

		g.Expect(actualPayload).To(Equal(bytesByID[id]))
	}
}

func TestClient_FetchError(t *testing.T) {
	g := NewGomegaWithT(t)

	ctx := context.Background()

	ctrl := gomock.NewController(t)
	s3Mock := mocks3.NewMockS3Client(ctrl)
	s3Mock.EXPECT().GetObject(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, _ ...func(options *s3.Options)) (*s3.GetObjectOutput, error) {
		return nil, errors.New("error connecting to s3")
	}).Times(1)

	c := client[string]{
		s3Bucket: testBucketName,
		s3Client: s3Mock,
	}

	idx := ObjectIndex{File: "1234", Offset: 0, Length: 120}
	b, err := c.Fetch(ctx, idx)
	g.Expect(err).To(MatchError("failed to download object from file test-bucket/1234 bytes=0-119: error connecting to s3"))
	g.Expect(b).To(BeNil())
}
