package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic/msgregistry"
	"github.com/julienschmidt/httprouter"
	stan "github.com/nats-io/stan.go"
	"github.com/noptics/golog"
	"github.com/noptics/protofiles"
	"github.com/noptics/streamer/registrygrpc"
)

const usage = `{"message":"path not found","usage":[{"description":"watch messages for a particular channel","path":"/:cluster/:channel","method":"GET","example":"GET /nats1/payoutRequests","response":{"cluster":"nats1","channel":"payoutRequests","files":[{"name":"request.proto","data":"<base64 encoded file data>"}]}}]}`

type RESTServer struct {
	rc      registrygrpc.ProtoRegistryClient
	sc      stan.Conn
	l       golog.Logger
	errChan chan error
	finish  chan struct{}
	wg      *sync.WaitGroup
	hs      http.Server
}

type RESTError struct {
	Message     string `json:"message"`
	Description string `json:"description,omitempty"`
	Details     string `json:"details"`
}

type RESTMessage struct {
	Error    *RESTError  `json:"error,omitempty"`
	NatsMeta *NATSMeta   `json:"natsMeta,omitempty"`
	Message  interface{} `json:"message"`
}

type NATSMeta struct {
	Sequence  uint64    `json:"sequence"`
	Timestamp time.Time `json:"timestamp"`
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func NewRestServer(rc registrygrpc.ProtoRegistryClient, sc stan.Conn, port string, errChan chan error, l golog.Logger) *RESTServer {
	rs := &RESTServer{
		rc:      rc,
		sc:      sc,
		l:       l,
		finish:  make(chan struct{}),
		wg:      &sync.WaitGroup{},
		errChan: errChan,
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

func (rs *RESTServer) wrapRoute(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	rs.wg.Add(1)
	defer rs.wg.Done()
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		rs.l.Errorw("error handling websocket connection", "error", err.Error())
		return
	}
	defer conn.Close()

	rs.l.Infow("new websocket connection", "source", conn.RemoteAddr())

	// attempt to get the schema from the registry
	files, err := rs.rc.GetFiles(context.Background(),
		&registrygrpc.GetFilesRequest{
			Cluster: ps.ByName("cluster"),
			Channel: ps.ByName("channel"),
		})

	if err != nil {
		rs.l.Debugw("error getting schema files from registry", "error", err)
		conn.WriteJSON(&RESTMessage{Error: &RESTError{Message: "unable to get schema files from registry", Details: err.Error()}})
		return
	}

	if len(files.Files) == 0 {
		rs.l.Debugw("no files in the registry", "cluster ", ps.ByName("cluster"), "channel", ps.ByName("channel"))
		conn.WriteJSON(&RESTMessage{Error: &RESTError{Message: "no files in registry", Details: fmt.Sprintf("cluster: %s, channel: %s", ps.ByName("cluster"), ps.ByName("channel"))}})
		return
	}

	msg, err := rs.rc.GetMessage(context.Background(),
		&registrygrpc.GetMessageRequest{
			Cluster: ps.ByName("cluster"),
			Channel: ps.ByName("channel"),
		})

	if err != nil {
		rs.l.Debugw("error getting channel message from registry", "error", err)
		conn.WriteJSON(&RESTMessage{Error: &RESTError{Message: "unable to get channgel message from registry", Details: err.Error()}})
		return
	}

	if len(msg.Name) == 0 {
		rs.l.Debugw("no message type set for channel", "cluster ", ps.ByName("cluster"), "channel", ps.ByName("channel"))
		conn.WriteJSON(&RESTMessage{Error: &RESTError{Message: "no message type set for channel", Details: fmt.Sprintf("cluster: %s, channel: %s", ps.ByName("cluster"), ps.ByName("channel"))}})
		return
	}

	// build the dynamic registry
	store := protofiles.NewStore()
	names := []string{}
	for _, f := range files.Files {
		store.Add(f.Name, f.Data)
		names = append(names, f.Name)
	}

	p := &protoparse.Parser{}
	p.Accessor = store.GetReadCloser

	ds, err := p.ParseFiles(names...)
	if err != nil {
		rs.l.Debugw("error decoding file data from registry", "error", err)
		conn.WriteJSON(&RESTMessage{Error: &RESTError{Message: "error decoding file data from registry", Details: err.Error()}})
		return
	}

	registry := msgregistry.NewMessageRegistryWithDefaults()
	for _, d := range ds {
		rs.l.Debugw("adding file", "name", d.GetName())
		registry.AddFile("noptics.io", d)
		mts := d.GetMessageTypes()
		for _, m := range mts {
			fmt.Println(m.GetFullyQualifiedName())
		}
	}

	msgPath := "noptics.io/" + msg.Name

	rs.l.Debugw("resolve", "path", msgPath)
	_, err = registry.Resolve(msgPath)
	if err != nil {
		rs.l.Debugw("error resolving message type from registry", "error", err)
		conn.WriteJSON(&RESTMessage{Error: &RESTError{Message: "error resolving message type from registry", Details: err.Error()}})
		return
	}

	// setup the nats subscription
	sc, err := rs.sc.Subscribe(ps.ByName("channel"), func(m *stan.Msg) {
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
		rs.l.Debugw("write error", "error", err)

	}, stan.MaxInflight(1))

	// handle a client disconnect
	closeChan := make(chan struct{})
	conn.SetCloseHandler(func(code int, text string) error {
		defer close(closeChan)
		message := websocket.FormatCloseMessage(code, "")
		conn.WriteControl(websocket.CloseMessage, message, time.Now().Add(1*time.Second))
		return nil
	})

	// wait until the connection is closed or we are told to stop
	select {
	case <-closeChan:
		rs.l.Debug("client disconnected closed")
	case <-rs.finish:
		rs.l.Debug("that's a wrap")
		conn.Close()
	}

	// unsub
	sc.Close()
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
	r.GET("/:cluster/:channel", rs.wrapRoute)

	r.NotFound = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(404)
		w.Write([]byte(usage))
	})

	return r
}
