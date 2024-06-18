package s3batchstore

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/oklog/ulid/v2"
)

// version is used to prefix the file name, so that we can change how the files are read in the future
const version string = "v1"

// TempFile creates a temp file in the filesystem, and is used to store the contents that will be uploaded to s3.
// This way we avoid having all the bytes in memory.
// This will also keep track of the indexes for each slice of bytes, in order to know where each of them are located
// TempFile is not thread safe, if you expect to make concurrent calls to Append, you should protect it.
// K represents the type of IDs for the objects that will be uploaded
type TempFile[K comparable] struct {
	fileName  string
	file      *os.File
	createdOn time.Time
	tags      map[string]string

	readonly  bool
	count     uint   // How many items are currently saved in the file
	bytesSize uint64 // The size of the actual file that we are storing
	offset    uint64 // The current offset in the file
	indexes   map[K]ObjectIndex
}

type ObjectIndex struct {
	File   string `json:"file"`
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

// why this method is an override of the NewTempFile method?
func (c *client[K]) NewTempFile(tags map[string]string) (*TempFile[K], error) {
	return NewTempFile[K](tags)
}

func NewTempFile[K comparable](tags map[string]string) (*TempFile[K], error) {
	fileName := ulid.Make().String()

	file, err := os.CreateTemp(os.TempDir(), fileName)
	if err != nil {
		return nil, err
	}

	return &TempFile[K]{
		fileName:  version + "/" + timeToFilePath(time.Now()) + "/" + fileName,
		file:      file,
		createdOn: time.Now(),
		tags:      tags,
		indexes:   map[K]ObjectIndex{},
	}, nil
}

// Append will take an id, and the slice of bytes of the Object, and append it to the temp file.
// This will also store the associated ObjectIndex information for this slice of bytes,
// telling where the object is located in this file (file, offset, length)
// This method is not thread safe, if you expect to make concurrent calls to Append, you should protect it.
func (f *TempFile[K]) Append(id K, bytes []byte) error {
	length := uint64(len(bytes))

	if f.readonly {
		return fmt.Errorf("file %s is readonly", f.fileName)
	}

	// Increment counters/metrics
	f.count++
	f.bytesSize += length

	// Append to file
	bytesWritten, err := f.file.Write(bytes)
	if err != nil {
		return fmt.Errorf("failed to write %d bytes (%d written) to file %s: %w", length, bytesWritten, f.file.Name(), err)
	}

	// Add index
	f.indexes[id] = ObjectIndex{
		File:   f.fileName,
		Offset: f.offset,
		Length: length,
	}
	f.offset += length

	return nil
}

// Name returns the fileName
func (f *TempFile[K]) Name() string {
	return f.fileName
}

// Tags returns the tags associated with this file
func (f *TempFile[K]) Tags() map[string]string {
	return f.tags
}

// Age returns the duration since this file has been started
// do we need this method? we are just using it in tests, maybe we can remove it and remove the created on attribute.
func (f *TempFile[K]) Age() time.Duration {
	return time.Since(f.createdOn)
}

// Count returns the number of items stored in this file
// same than Age method.
func (f *TempFile[K]) Count() uint {
	return f.count
}

// Size returns the size of the file contents in bytes
// same than Age method.
func (f *TempFile[K]) Size() uint64 {
	return f.bytesSize
}

// Indexes returns the indexes that the file is holding
func (f *TempFile[K]) Indexes() map[K]ObjectIndex {
	return f.indexes
}

// Close will delete the file, as it is no longer needed, and given that these files may be really large,
// we want to avoid having then live in the os for a long period of time.
func (f *TempFile[K]) Close() error {
	// This is a temp file, so on Close we delete it.
	return os.Remove(f.file.Name())
}

// MetaFileKey  returns the key to be used for the json meta file
func (f *TempFile[K]) MetaFileKey() string {
	return f.fileName + ".meta.json"
}

// readOnly logically closes the file by not accepting more appends, and returns the os.File used to upload the file to s3
func (f *TempFile[K]) readOnly() (*os.File, error) {
	// Set file pointer to beginning
	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	f.readonly = true
	return f.file, nil
}

// timeToFilePath returns the time formatted as yyyy/mm/dd/hh, in UTC timezone
func timeToFilePath(t time.Time) string {
	return t.UTC().Format("2006/01/02/15")
}
