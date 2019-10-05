package protofiles

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
)

type Store struct {
	fileData map[string][]byte
}

func NewStore() *Store {
	return &Store{fileData: map[string][]byte{}}
}

func (s *Store) Add(name string, data []byte) {
	s.fileData[name] = data
}

func (s *Store) Has(name string) bool {
	_, ok := s.fileData[name]
	return ok
}

func (s *Store) Get(name string) ([]byte, error) {
	d, ok := s.fileData[name]
	if !ok {
		return nil, errors.New("unable to find file")
	}
	return d, nil
}

func (s *Store) GetReadCloser(name string) (io.ReadCloser, error) {
	d, ok := s.fileData[name]
	if !ok {
		return nil, errors.New("unable to find file")
	}
	return ioutil.NopCloser(bytes.NewReader(d)), nil
}
