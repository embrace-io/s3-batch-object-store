# s3-batch-object-store

`s3-batch-object-store` is a Go module that allows for batch uploading of objects to a single S3 file and retrieving 
each object separately using the AWS S3 API, fetching only the bytes for that specific object

## Features

- Batch upload multiple objects into a single S3 file, reducing the number of PUT operations.
- Retrieve individual objects using index information (byte offset and length).

## Installation

To install the module, use `go get`:

```sh
go get github.com/embrace-io/s3-batch-object-store
```

## Usage

### Example

Here is a basic example demonstrating how to use the `s3-batch-object-store` module:


```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/embrace-io/s3-batch-object-store"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Load the AWS configuration
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic("unable to load AWS SDK config: " + err.Error())
	}

	// Create the s3 batch store Client, with string as object IDs.
	client := s3batchstore.NewClient[string](awsCfg, "my-bucket")

	// Example objects to upload
	objects := map[string][]byte{
		"object1": []byte("This is the content of object1."),
		"object2": []byte("This is the content of object2."),
		"object3": []byte("This is the content of object3."),
	}

	// Create the new temp file
	file, err := client.NewTempFile(map[string]string{
		// You can add any tags and these will be set in the s3 file.
		// This can be used for example to set TTL rules, and automatically delete the files.
		"retention-days": "14",
	})
	if err != nil {
		panic("unable to create temp file: " + err.Error())
	}

	// Append all the objects to the file:
	for id, obj := range objects {
		err = file.Append(id, obj)
		if err != nil {
			panic("unable to create temp file: " + err.Error())
		}
	}

	// Upload the objects
	err = client.UploadToS3(ctx, file, true)
	if err != nil {
		panic("failed to upload object: " + err.Error())
	}

	// At this point the file.Indexes() can be stored to be used later to retrieve the objects.

	// Retrieve an object
	indexes := file.Indexes()
	content, err := client.Fetch(ctx, indexes["object2"])
	if err != nil {
		panic("failed to retrieve object, " + err.Error())
	}

	fmt.Printf("Contents of object2:\n%s", content)
	// Contents of object2:
	// This is the content of object2.
}


```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

TODO 
