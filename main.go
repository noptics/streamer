package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/noptics/golog"
	"github.com/noptics/streamer/registrygrpc"
	"google.golang.org/grpc"
)

func main() {
	var l golog.Logger
	if len(os.Getenv("DEBUG")) != 0 {
		l = golog.StdOut(golog.LEVEL_DEBUG)
	} else {
		l = golog.StdOut(golog.LEVEL_ERROR)
	}

	l.Init()
	defer l.Finish()

	c := newContext()

	l.Info("starting server")

	// Connect to the registry
	registry := os.Getenv("REGISTRY_SERVICE")
	if len(registry) == 0 {
		l.Error("must provide REGISTRY_SERVICE")
		os.Exit(1)
	}

	l.Infow("connecting to registry service", "addr", registry)
	conn, err := grpc.Dial(registry, grpc.WithInsecure())
	if err != nil {
		l.Errorw("error connecting to registry service", "error", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	rc := registrygrpc.NewProtoRegistryClient(conn)

	l.Info("successfully connected to the registry service")

	errChan := make(chan error)

	// start the grpc server
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "7785"
	}

	gs, err := NewGRPCServer(rc, grpcPort, errChan, l)
	if err != nil {
		l.Infow("unable to start grpc server", "error", err.Error())
		os.Exit(1)
	}

	// start the rest server
	rsPort := os.Getenv("REST_PORT")
	if rsPort == "" {
		rsPort = "7786"
	}

	rs := NewRestServer(rc, rsPort, errChan, l, c)

	l.Info("started")

	// go until told to stop
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-sigs:
	case <-errChan:
		l.Infow("error", "error", err.Error())
	}

	l.Info("shutting down")

	gs.Stop()

	err = rs.Stop()
	if err != nil {
		l.Infow("error shutting down rest server", "error", err.Error())
	}

	l.Info("finished")
}
