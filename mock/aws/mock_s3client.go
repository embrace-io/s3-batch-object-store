// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/embrace-io/s3-batch-object-store (interfaces: S3Client)
//
// Generated by this command:
//
//	mockgen -destination=./mock/aws/mock_s3client.go -package=mocks3 . S3Client
//

// Package mocks3 is a generated GoMock package.
package mocks3

import (
	context "context"
	reflect "reflect"

	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	gomock "go.uber.org/mock/gomock"
)

// MockS3Client is a mock of S3Client interface.
type MockS3Client struct {
	ctrl     *gomock.Controller
	recorder *MockS3ClientMockRecorder
}

// MockS3ClientMockRecorder is the mock recorder for MockS3Client.
type MockS3ClientMockRecorder struct {
	mock *MockS3Client
}

// NewMockS3Client creates a new mock instance.
func NewMockS3Client(ctrl *gomock.Controller) *MockS3Client {
	mock := &MockS3Client{ctrl: ctrl}
	mock.recorder = &MockS3ClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockS3Client) EXPECT() *MockS3ClientMockRecorder {
	return m.recorder
}

// DeleteObjects mocks base method.
func (m *MockS3Client) DeleteObjects(arg0 context.Context, arg1 *s3.DeleteObjectsInput, arg2 ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DeleteObjects", varargs...)
	ret0, _ := ret[0].(*s3.DeleteObjectsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteObjects indicates an expected call of DeleteObjects.
func (mr *MockS3ClientMockRecorder) DeleteObjects(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteObjects", reflect.TypeOf((*MockS3Client)(nil).DeleteObjects), varargs...)
}

// GetObject mocks base method.
func (m *MockS3Client) GetObject(arg0 context.Context, arg1 *s3.GetObjectInput, arg2 ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetObject", varargs...)
	ret0, _ := ret[0].(*s3.GetObjectOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetObject indicates an expected call of GetObject.
func (mr *MockS3ClientMockRecorder) GetObject(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetObject", reflect.TypeOf((*MockS3Client)(nil).GetObject), varargs...)
}

// PutObject mocks base method.
func (m *MockS3Client) PutObject(arg0 context.Context, arg1 *s3.PutObjectInput, arg2 ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PutObject", varargs...)
	ret0, _ := ret[0].(*s3.PutObjectOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PutObject indicates an expected call of PutObject.
func (mr *MockS3ClientMockRecorder) PutObject(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutObject", reflect.TypeOf((*MockS3Client)(nil).PutObject), varargs...)
}
