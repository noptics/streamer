package main

import (
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic/msgregistry"
	"github.com/noptics/golog"
	"github.com/noptics/protofiles"
	"github.com/noptics/streamer/registrygrpc"
)

func getProtoDynamicRegistry(files []*registrygrpc.File, l golog.Logger) (*msgregistry.MessageRegistry, error) {
	// build the dynamic registry
	store := protofiles.NewStore()
	names := []string{}
	for _, f := range files {
		store.Add(f.Name, f.Data)
		names = append(names, f.Name)
	}

	p := &protoparse.Parser{}
	p.Accessor = store.GetReadCloser

	ds, err := p.ParseFiles(names...)
	if err != nil {
		l.Debugw("error decoding file data from registry", "error", err)
		return nil, err
	}

	registry := msgregistry.NewMessageRegistryWithDefaults()
	for _, d := range ds {
		l.Debugw("adding file", "name", d.GetName())
		registry.AddFile("noptics.io", d)
	}

	return registry, nil
}
