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

TODO

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

TODO 

