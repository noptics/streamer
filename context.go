package main

import (
	"os"
	"runtime"
)

type Context struct {
	GoVersion string `json:"go"`
	Version   string `json:"version"`
	Host      string `json:"host"`
	RESTPort  string `json:"rest_port"`
	GRPCPort  string `json:"grpc_port"`
	Commit    string `json:"git_commit"`
}

func newContext() *Context {
	return &Context{
		Version:   os.Getenv("VERSION"),
		Commit:    os.Getenv("COMMIT"),
		GoVersion: runtime.Version(),
	}
}
