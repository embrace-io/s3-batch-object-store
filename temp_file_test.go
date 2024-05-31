package s3batchstore

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

const testBucketName = "test-bucket"

var testTags = map[string]string{
	"retention-days": "14",
}

func TestFile_WriteError(t *testing.T) {
	g := NewGomegaWithT(t)

	file, err := NewTempFile(testTags)
	g.Expect(err).ToNot(HaveOccurred())
	defer func() { _ = file.Close() }()

	obj := &TestObject{ID: "4", Value: "contents"}
	compressed, err := marshalAndCompress(obj)
	g.Expect(err).ToNot(HaveOccurred())

	err = file.Append(obj.ID, compressed)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(file.Name()).To(Equal(file.fileName))
	g.Expect(file.Indexes()[obj.ID].Offset).To(Equal(uint64(0)))
	g.Expect(file.Indexes()[obj.ID].Length).To(BeNumerically(">", 0))

	// If file is closed, it won't be able to write more:
	g.Expect(file.file.Close()).ToNot(HaveOccurred())

	err = file.Append(obj.ID, compressed)
	fileName := file.file.Name()
	g.Expect(err).To(MatchError(fmt.Sprintf("failed to write %d bytes (0 written) to file %s: write %s: file already closed", len(compressed), fileName, fileName)))
}

func TestFile_ReadOnly(t *testing.T) {
	g := NewGomegaWithT(t)

	file, err := NewTempFile(testTags)
	g.Expect(err).ToNot(HaveOccurred())
	defer func() { _ = file.Close() }()

	obj := &TestObject{ID: "4", Value: "contents"}
	compressed, err := marshalAndCompress(obj)
	g.Expect(err).ToNot(HaveOccurred())

	// Store one object, then ask for the readonly file and try to store one more object
	err = file.Append(obj.ID, compressed)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(file.Indexes()[obj.ID].Offset).To(Equal(uint64(0)))
	g.Expect(file.Indexes()[obj.ID].Length).To(BeNumerically(">", 0))

	roFile, err := file.readOnly()
	g.Expect(roFile).ToNot(BeNil())
	g.Expect(err).To(BeNil())

	err = file.Append(obj.ID, compressed)
	g.Expect(err).To(MatchError(fmt.Sprintf("file %s is readonly", file.fileName)))
}

func TestFile_ReadOnlyError(t *testing.T) {
	g := NewGomegaWithT(t)

	file, err := NewTempFile(testTags)
	g.Expect(err).ToNot(HaveOccurred())
	defer func() { _ = file.Close() }()

	obj := &TestObject{ID: "4", Value: "contents"}
	compressed, err := marshalAndCompress(obj)
	g.Expect(err).ToNot(HaveOccurred())

	err = file.Append(obj.ID, compressed)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(file.Indexes()[obj.ID].Offset).To(Equal(uint64(0)))
	g.Expect(file.Indexes()[obj.ID].Length).To(BeNumerically(">", 0))

	// If file is closed, we won't be able to get the readOnly file
	g.Expect(file.file.Close()).ToNot(HaveOccurred())

	roFile, err := file.readOnly()
	g.Expect(roFile).To(BeNil())
	g.Expect(err).To(MatchError(fmt.Sprintf("seek %s: file already closed", file.file.Name())))
}

func TestTimeToFilePath(t *testing.T) {
	g := NewGomegaWithT(t)
	tt := time.Date(2021, 10, 8, 02, 10, 14, 33, time.UTC)
	g.Expect(timeToFilePath(tt)).To(Equal("2021/10/08/02"))
}

// TestObject represents a document that may be uploaded to s3 and fetched from s3
type TestObject struct {
	ID    ObjectID `json:"id"`
	Value string   `json:"value"`
}

func marshalAndCompress(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return gzipCompress(b)
}

func gzipCompress(data []byte) ([]byte, error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write(data); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func gzipDecompress(data []byte) ([]byte, error) {
	reader := bytes.NewReader(data)
	gz, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(gz)
}
