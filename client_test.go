package s3batchstore

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	. "github.com/onsi/gomega"
)

func TestNewClient(t *testing.T) {
	g := NewGomegaWithT(t)

	c := NewClient(aws.Config{}, testBucketName)
	g.Expect(c).ToNot(BeNil())
	g.Expect(c.(*client).s3Client).ToNot(BeNil())
	g.Expect(c.(*client).s3Bucket).To(Equal(testBucketName))
}
