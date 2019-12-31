package stanconnection

import (
	"errors"
	"fmt"

	"github.com/lithammer/shortuuid"
	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
)

// Wraps a connection to the stan cluster
type StanConnection struct {
	natsAddress    string
	clusterID      string
	natsConnection *nats.Conn
	stanConnection stan.Conn
	clientID       string
	subscriptions  map[string]stan.Subscription
}

func New(natsAddress, clusterID string) *StanConnection {
	return &StanConnection{
		natsAddress: natsAddress,
		clusterID:   clusterID,
		clientID:    fmt.Sprintf("%s-%s", shortuuid.New(), "streamer"),
	}
}

func (sc *StanConnection) Close() {
	for _, s := range sc.subscriptions {
		s.Close()
	}
	sc.natsConnection.Close()
	sc.stanConnection.Close()
}

func (sc *StanConnection) Connect() (err error) {
	sc.natsConnection, err = nats.Connect(sc.natsAddress)
	if err != nil {
		return
	}

	sc.stanConnection, err = stan.Connect(sc.clusterID, sc.clientID, stan.NatsConn(sc.natsConnection))
	if err != nil {
		sc.natsConnection.Close()
	}
	return
}

func (sc *StanConnection) Subscribe(channel string, process func(m *stan.Msg), options []stan.SubscriptionOption) error {
	if sc.stanConnection == nil {
		return errors.New("Must connect to stan first")
	}

	if sc.subscriptions == nil {
		sc.subscriptions = map[string]stan.Subscription{}
	}

	if _, ok := sc.subscriptions[channel]; ok {
		return fmt.Errorf("Subscriptiont to channel %s exists. Must unscribe first.", channel)
	}

	sub, err := sc.stanConnection.Subscribe(channel, process, options...)

	if err != nil {
		return err
	}

	sc.subscriptions[channel] = sub

	return nil
}

func (sc *StanConnection) Unsubscribe(channel string) {
	if sc.subscriptions == nil {
		return
	}

	if _, ok := sc.subscriptions[channel]; ok {
		sc.subscriptions[channel].Close()
		delete(sc.subscriptions, channel)
	}
}
