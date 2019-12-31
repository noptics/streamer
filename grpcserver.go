package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/gogo/protobuf/proto"
	stan "github.com/nats-io/stan.go"
	"github.com/noptics/golog"
	"github.com/noptics/streamer/registrygrpc"
	"github.com/noptics/streamer/stanconnection"
	"github.com/noptics/streamer/streamergrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCServer struct {
	l       golog.Logger
	rc      registrygrpc.ProtoRegistryClient
	grpcs   *grpc.Server
	errChan chan error
}

func NewGRPCServer(rc registrygrpc.ProtoRegistryClient, port string, errChan chan error, l golog.Logger) (*GRPCServer, error) {
	gs := &GRPCServer{
		rc:      rc,
		l:       l,
		errChan: errChan,
		grpcs:   grpc.NewServer(),
	}

	l.Infow("starting grpc service", "port", port)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return nil, err
	}

	go func() {
		if err = gs.grpcs.Serve(lis); err != nil {
			gs.errChan <- err
		}
	}()

	return gs, nil
}

func (gs *GRPCServer) Stop() {
	gs.grpcs.GracefulStop()
}

func (gs *GRPCServer) Stream(stream streamergrpc.Messages_StreamServer) error {
	var sc *stanconnection.StanConnection
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch in.Command {
		case streamergrpc.Command_START:
			if sc != nil {
				// ignore this message
				continue
			}

			// start options
			var natsAddress, cluster, channel string
			var startDur time.Duration

			if na, ok := in.Options["natsAddress"]; ok {
				natsAddress = na
			} else {
				return status.Error(codes.InvalidArgument, "must provide natsAddress, stanCluster, and channel options")
			}

			if c, ok := in.Options["stanCluster"]; ok {
				cluster = c
			} else {
				return status.Error(codes.InvalidArgument, "must provide natsAddress, stanCluster, and channel options")
			}

			if c, ok := in.Options["channel"]; ok {
				channel = c
			} else {
				return status.Error(codes.InvalidArgument, "must provide natsAddress, stanCluster, and channel options")
			}

			if s, ok := in.Options["start"]; ok {
				startDur, err = time.ParseDuration(s)
				if err != nil {
					return status.Error(codes.InvalidArgument, fmt.Sprintf("provided start duration string not valid: %s", err.Error()))
				}
			}

			// Get data from the registry
			data, err := gs.rc.GetChannelData(context.Background(),
				&registrygrpc.GetChannelDataRequest{
					Cluster: cluster,
					Channel: channel,
				})

			if err != nil {
				return status.Error(codes.InvalidArgument, fmt.Sprintf("unable to get schema files from registry: %s", err.Error()))
			}

			// Make sure we have schema files, messages type set, and that we can resolve the message
			if len(data.Files) == 0 {
				return status.Error(codes.FailedPrecondition, fmt.Sprintf("no files in registry for %s %s", cluster, channel))
			}

			if len(data.Message) == 0 {
				return status.Error(codes.FailedPrecondition, fmt.Sprintf("no message type set for channel %s", channel))
			}

			registry, err := getProtoDynamicRegistry(data.Files, gs.l)
			if err != nil {
				return status.Error(codes.Unknown, fmt.Sprintf("error resolving message type from registry: %s", err.Error()))
			}

			msgPath := "noptics.io/" + data.Message

			if _, err = registry.Resolve(msgPath); err != nil {
				return status.Error(codes.Unknown, fmt.Sprintf("error resolving message type from registry: %s", err.Error()))
			}

			sc = stanconnection.New(natsAddress, cluster)
			err = sc.Connect()
			if err != nil {
				return status.Error(codes.Unknown, fmt.Sprintf("error connecting to stan cluster: %s", err.Error()))
			}

			defer sc.Close()
			opts := []stan.SubscriptionOption{
				stan.MaxInflight(1),
			}
			if startDur != 0 {
				opts = append(opts, stan.StartAtTimeDelta(startDur))
			}

			// TODO: set this up so that if we run into errors in the message handling routine we close the grpc conn
			err = sc.Subscribe(channel, func(m *stan.Msg) {
				rm := &streamergrpc.Message{NatsMeta: &streamergrpc.NatsMeta{
					Sequence:  m.MsgProto.Sequence,
					Timestamp: time.Unix(0, m.Timestamp).Format(time.RFC3339),
				}}

				resolved, _ := registry.Resolve(msgPath)

				err := proto.Unmarshal(m.Data, resolved)
				if err != nil {
					gs.l.Errorw("error decoding message", "cluster", cluster, "channel", channel, "message", rm)
					return
				}

				j, err := json.Marshal(resolved)
				if err != nil {
					gs.l.Errorw("error decoding message", "cluster", cluster, "channel", channel, "message", rm, "error", err)
					return
				}
				rm.Data = string(j)

				err = stream.Send(rm)
				if err != nil {
					gs.l.Errorw("error sending message", "cluster", cluster, "channel", channel, "message", rm, "error", err)
				}

			}, opts)

			if err != nil {
				return status.Error(codes.Unknown, err.Error())
			}

		case streamergrpc.Command_FILTER_ADD:
			// not implemented yet
		case streamergrpc.Command_FILTER_REMOVE:
			// not implemeted yet
		case streamergrpc.Command_STOP:
			// stop streaming
			return nil
		}
	}
}
