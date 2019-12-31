package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"
	"github.com/julienschmidt/httprouter"
	stan "github.com/nats-io/stan.go"
	"github.com/noptics/golog"
	"github.com/noptics/streamer/registrygrpc"
	"github.com/noptics/streamer/stanconnection"
)

const usage = `{"message":"path not found","usage":[{"description":"watch messages for a particular channel","path":"/:cluster/:channel","method":"GET","example":"GET /nats1/payoutRequests","response":{"cluster":"nats1","channel":"payoutRequests","files":[{"name":"request.proto","data":"<base64 encoded file data>"}]}}]}`

// Wraps the routes and handlers
type RESTServer struct {
	rc      registrygrpc.ProtoRegistryClient
	l       golog.Logger
	errChan chan error
	finish  chan struct{}
	wg      *sync.WaitGroup
	hs      http.Server
	context *Context
}

// RESTError is the standard structure for rest api errors
type RESTError struct {
	Message     string `json:"message"`
	Description string `json:"description,omitempty"`
	Details     string `json:"details"`
}

// RESTMessage is the message streaming reply structure
type RESTMessage struct {
	Error    *RESTError  `json:"error,omitempty"`
	NatsMeta *NATSMeta   `json:"natsMeta,omitempty"`
	Message  interface{} `json:"message"`
}

// Metadata about the nats message
type NATSMeta struct {
	Sequence  uint64    `json:"sequence"`
	Timestamp time.Time `json:"timestamp"`
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func NewRestServer(rc registrygrpc.ProtoRegistryClient, port string, errChan chan error, l golog.Logger, context *Context) *RESTServer {
	rs := &RESTServer{
		rc:      rc,
		l:       l,
		finish:  make(chan struct{}),
		wg:      &sync.WaitGroup{},
		errChan: errChan,
		context: context,
	}

	rs.hs = http.Server{Addr: ":" + port, Handler: rs.Router()}

	l.Infow("starting rest server", "port", port)
	go func() {
		if err := rs.hs.ListenAndServe(); err != nil {
			rs.errChan <- err
		}
	}()

	return rs
}

func (rs *RESTServer) WebSocket(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	rs.wg.Add(1)
	defer rs.wg.Done()

	natsAddress := r.URL.Query().Get("natsAddress")
	stanCluster := r.URL.Query().Get(("stanCluster"))

	if len(natsAddress) == 0 && len(stanCluster) == 0 {
		rs.l.Errorw("error invalid parameters provided", "natsAddress", natsAddress, "stanCluster", stanCluster)
		setHeaders(w, r.Header)
		w.WriteHeader(400)
		w.Write([]byte(`{"message":"error","error":{"message": "invalid parameters provided", "details":"must provide natsAddress and stanClsuter"}}`))
		return
	}

	start := r.URL.Query().Get("start")
	var startDur time.Duration
	var err error
	if len(start) != 0 {
		startDur, err = time.ParseDuration(start)
		if err != nil {
			rs.l.Errorw("error invalid duration provided", "start", start)
			setHeaders(w, r.Header)
			w.WriteHeader(400)
			w.Write([]byte(`{"message":"error","error":{"message": "invalid duration string provided"}}`))
			return
		}
	}

	rs.l.Debugw("query params", "natsAddress", natsAddress, "stanCluster", stanCluster)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		rs.l.Errorw("error handling websocket connection", "error", err.Error())
		return
	}
	defer conn.Close()

	rs.l.Infow("new websocket connection", "source", conn.RemoteAddr())

	// attempt to get the schema from the registry
	data, err := rs.rc.GetChannelData(context.Background(),
		&registrygrpc.GetChannelDataRequest{
			Cluster: ps.ByName("cluster"),
			Channel: ps.ByName("channel"),
		})

	if err != nil {
		rs.l.Debugw("error getting schema from registry", "error", err)
		conn.WriteJSON(&RESTMessage{
			Error:   &RESTError{Message: "unable to get schema files from registry", Details: err.Error()},
			Message: "error",
		})
		return
	}

	if len(data.Files) == 0 {
		rs.l.Debugw("no files in the registry", "cluster ", ps.ByName("cluster"), "channel", ps.ByName("channel"))
		conn.WriteJSON(&RESTMessage{
			Error:   &RESTError{Message: "no files in registry", Details: fmt.Sprintf("cluster: %s, channel: %s", ps.ByName("cluster"), ps.ByName("channel"))},
			Message: "error",
		})
		return
	}

	if len(data.Message) == 0 {
		rs.l.Debugw("no message type set for channel", "cluster ", ps.ByName("cluster"), "channel", ps.ByName("channel"))
		conn.WriteJSON(&RESTMessage{
			Error:   &RESTError{Message: "no message type set for channel", Details: fmt.Sprintf("cluster: %s, channel: %s", ps.ByName("cluster"), ps.ByName("channel"))},
			Message: "error",
		})
		return
	}

	registry, err := getProtoDynamicRegistry(data.Files, rs.l)
	if err != nil {
		conn.WriteJSON(&RESTMessage{
			Error:   &RESTError{Message: "error resolving message type from registry", Details: err.Error()},
			Message: "error",
		})
		return
	}

	msgPath := "noptics.io/" + data.Message

	rs.l.Debugw("resolve", "path", msgPath)
	_, err = registry.Resolve(msgPath)
	if err != nil {
		rs.l.Debugw("error resolving message type from registry", "error", err)
		conn.WriteJSON(&RESTMessage{
			Error:   &RESTError{Message: "error resolving message type from registry", Details: err.Error()},
			Message: "error",
		})
		return
	}

	sc := stanconnection.New(natsAddress, stanCluster)
	err = sc.Connect()

	if err != nil {
		rs.l.Debugw("error connecting to stan cluster", "error", err)
		conn.WriteJSON(&RESTMessage{
			Error:   &RESTError{Message: "error connecting to stan cluster", Details: err.Error()},
			Message: "error",
		})
		return
	}

	defer sc.Close()

	// setup the nats subscription
	opts := []stan.SubscriptionOption{
		stan.MaxInflight(1),
	}
	if len(start) != 0 {
		opts = append(opts, stan.StartAtTimeDelta(startDur))
	}

	err = sc.Subscribe(ps.ByName("channel"), func(m *stan.Msg) {
		rm := RESTMessage{NatsMeta: &NATSMeta{
			Sequence:  m.MsgProto.Sequence,
			Timestamp: time.Unix(0, m.Timestamp),
		}}

		resolved, _ := registry.Resolve(msgPath)

		err := proto.Unmarshal(m.Data, resolved)
		if err != nil {
			rm.Error = &RESTError{
				Message: "error unmarshaling data",
				Details: err.Error(),
			}
		} else {
			rm.Message = resolved
		}

		err = conn.WriteJSON(rm)
		if err != nil {
			rs.l.Debugw("write error", "error", err)
		}

	}, opts)

	if err != nil {
		rs.l.Errorw("unable to setup stan subscription", "error", err)
		return
	}

	// handle a client disconnect
	closeChan := make(chan struct{})

	// MUST read the incoming messages - the above handler is called via the NextReader() function
	go func(c *websocket.Conn) {
		for {
			if _, _, err := c.NextReader(); err != nil {
				// gorilla treats close messages from the server as read errors.
				defer close(closeChan)
				// preserve the error, if there was indeed one...
				rs.l.Debugw("error reading incoming message", "error", err.Error())
				break
			}
		}
	}(conn)

	// wait until the connection is closed or we are told to stop
	select {
	case <-closeChan:
		rs.l.Debug("client disconnected, channel closed")
	case <-rs.finish:
		rs.l.Debug("that's a wrap")
	}
}

func (rs *RESTServer) ServerStatus(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	type RESTStatusData struct {
		Context
	}

	code := 200
	body, err := json.Marshal(&RESTStatusData{*rs.context})
	if err != nil {
		body = []byte(fmt.Sprintf(`{"message": "error marshalling response", "description":"unable to json encode data", "details":"%s"`, err.Error()))
		code = 500
	}

	setHeaders(w, r.Header)
	w.WriteHeader(code)
	if len(body) != 0 {
		w.Write(body)
	}
}

func setHeaders(w http.ResponseWriter, rh http.Header) {
	allowMethod := "GET, POST, PUT, DELETE, OPTIONS"
	allowHeaders := "Content-Type"
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Allow", allowMethod)
	w.Header().Set("Access-Control-Allow-Methods", allowMethod)
	w.Header().Set("Access-Control-Allow-Headers", allowHeaders)

	o := rh.Get("Origin")
	if o == "" {
		o = "*"
	}
	w.Header().Set("Access-Control-Allow-Origin", o)
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Content-Type", "application/json")
}

func (rs *RESTServer) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := rs.hs.Shutdown(ctx)
	close(rs.finish)
	rs.wg.Wait()
	return err
}

func (rs *RESTServer) Router() *httprouter.Router {
	r := httprouter.New()

	r.HandleOPTIONS = true
	r.GlobalOPTIONS = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setHeaders(w, r.Header)
		w.WriteHeader(http.StatusNoContent)
	})

	r.GET("/:cluster/:channel/stream", rs.WebSocket)
	r.GET("/", rs.ServerStatus)

	r.NotFound = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(404)
		w.Write([]byte(usage))
	})

	return r
}
