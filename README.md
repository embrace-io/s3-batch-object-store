# s3-batch-object-store

`s3-batch-object-store` is a Go module that allows for batch uploading of objects to a single S3 file and retrieving 
each object separately using the AWS S3 API, fetching only the bytes for that specific object.

The method basically consists of appending multiple objects to a single file, keep the information of where each object
is placed in the file, and then upload one single file to s3 with many objects in it, reducing drastically the number 
of PUT operations needed to store a large number of objets.

Then, when you need to retrieve an object, you can use the index information to fetch that object and the GET call to s3
will only retrieve the bytes that correspond to that object, reducing the amount of data transferred.

This method of storage and retrieval is well suited for write-heavy workloads, where you want to fetch a small 
percentage of the stored objects later.
This storage approach also works well when you have objects of widely varying size.

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
		panic("failed to load AWS SDK config: " + err.Error())
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
		panic("failed to create temp file: " + err.Error())
	}

	// Append all the objects to the file:
	for id, obj := range objects {
		if err = file.Append(id, obj); err != nil {
			panic("failed to append object to temp file: " + err.Error())
		}
	}

	// You can check the file properties to decide when to upload a file:
	fmt.Printf("File is %s old, has %d objects, and is %d bytes long\n", file.Age(), file.Count(), file.Size())
	// File is 42.375µs old, has 3 objects, and is 93 bytes long

	// Upload the objects
	err = client.UploadFile(ctx, file, true)
	if err != nil {
		panic("failed to upload object: " + err.Error())
	}

	// At this point the file.Indexes() can be stored to be used later to retrieve the objects.
	fmt.Printf("File indexes:\n")
	for id, index := range file.Indexes() {
		fmt.Printf("objectID: %v, index: %+v\n", id, index)
	}

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
