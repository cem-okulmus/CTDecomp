package lib

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
)

// TODO: create an interface over the pubsub channels that allows it to be used in the program as if it was just a normal channel. this includes
//     * the work needed to set up the connection,
//     * check if a topic exists, and create it if needed (no need to use the webinterface all the time)
// 			func (c *Client) CreateTopic(ctx context.Context, topicID string) (*Topic, error)
// 			-> check if it exists, and delete it first if needed (should take care of existing messages)
//	   * check if a Subscription exists, and create it if needed
// 			- requries both the topic and the subscription name
//     * as well as cleaning up the channels before use
//			(removing any messsages still waiting) -> check if APIs for this exist

type Kind int

const (
	Sending Kind = iota
	Receiving
)

type Message interface {
	ToBytes() []byte
}

// PSChannel manages a connection to a pubsub channel. Note that this combines a topic and a subscription,
type PSChannel struct {
	kind    Kind   // defines the how the channel works
	topicID string // the topic to sent to
	subID   string // the subscription to listen
	client  *pubsub.Client
	topic   *pubsub.Topic
	sub     *pubsub.Subscription
	ctx     context.Context
}

// handle is a simple helper function to set up error message output
func handle(messsage string, err error) {
	if err != nil {
		log.Fatal(messsage, " : ", err)
	}
}

// SetupChannel only sets up connection to already existing channel
func SetupChannel(kind Kind, topicID string, subID string) PSChannel {
	var output PSChannel

	ctx := context.Background()

	// Sets your Google Cloud Platform project ID.

	fmt.Println("Connecting to a Channel of ", kind, " with topic name ", topicID)
	projectID := "candidatetd-dist"

	// Creates a client.
	client, err := pubsub.NewClient(ctx, projectID)
	handle("Failed to create client", err)

	output.ctx = ctx
	output.client = client
	output.kind = kind
	output.topicID = topicID
	output.subID = "sub" + strings.ReplaceAll(subID, ":", "")

	if output.kind == Sending { // setup a Topic
		topic := client.Topic(output.topicID)

		ok, err := topic.Exists(ctx)
		handle("Failed to check if topic "+topic.ID()+" exists", err)

		if !ok {
			log.Panicln("Topic doesn't exist", topicID)
		}

		output.topic = topic

	} else { // setup a Subscription to a topic
		sub := client.Subscription(output.subID)

		ok, err := sub.Exists(ctx)
		handle("Failed to check if subscription exists", err)

		if !ok {
			log.Panicln("subscription doesn't exist", topicID)
		}

		output.sub = sub
	}

	return output
}

// SetupAndCreateChannel creates a new pubsub channel
func SetupAndCreateChannel(kind Kind, topicID string, subID string) PSChannel {
	var output PSChannel

	ctx := context.Background()

	// Sets your Google Cloud Platform project ID.
	projectID := "candidatetd-dist"

	// Creates a client.
	fmt.Println("Creating a Channel of ", kind, " with topic name ", topicID)
	client, err := pubsub.NewClient(ctx, projectID)
	handle("Failed to create client", err)

	output.ctx = ctx
	output.client = client
	output.kind = kind
	output.topicID = topicID
	output.subID = "sub" + strings.ReplaceAll(subID, ":", "")

	if output.kind == Sending { // setup a Topic
		topic := client.Topic(output.topicID)

		ok, err := topic.Exists(ctx)
		handle("Failed to check if topic "+topic.ID()+" exists", err)

		// if ok {
		// 	fmt.Println("Topic exists, deleting")
		// 	err = topic.Delete(ctx)
		// 	handle("Failed to delete topic", err)
		// }

		if !ok {
			topic, err = client.CreateTopic(ctx, output.topicID)
			handle("Failed to create new topic", err)
		}

		output.topic = topic

	} else { // setup a Subscription to a topic
		topic := client.Topic(output.topicID)
		sub := client.Subscription(output.subID)

		ok, err := sub.Exists(ctx)
		handle("Failed to check if subscription exists", err)

		// if ok {
		// 	err = sub.Delete(ctx)
		// 	fmt.Println("Sub already exists, deleting")
		// 	handle("Failed to delete subscription", err)
		// }

		if !ok {
			// fmt.Println("Setting up the Sub")
			sub, err = client.CreateSubscription(ctx, output.subID, pubsub.SubscriptionConfig{Topic: topic})
			handle("Failed to create new subscription", err)
		}
		output.sub = sub
	}

	return output
}

// CloseChannel closes the respective pubsub channel
func (p *PSChannel) CloseChannel() {
	if p.kind == Sending {
		p.topic.Delete(p.ctx)
	} else {
		p.sub.Delete(p.ctx)
	}

	p.client.Close()
}

// Write sends some data over the topic, blocking operation
func (p PSChannel) Write(m Message) {
	if p.kind == Receiving {
		return // nothing to write if kind set to receive
	}
	out := m.ToBytes()
	// fmt.Println("sending data, ", out)

	res := p.topic.Publish(p.ctx, &pubsub.Message{
		Data: out,
	})

	_, err := res.Get(p.ctx)
	if err != nil {
		log.Fatal(err)
	}
}

// Receiving waits for the first message to arrive from the subscription and returns it.
// No more than one message is returned per function call
func (p PSChannel) Receiving() []byte {
	var output []byte
	if p.kind == Sending {
		return output // nothing to receive if kind set to send
	}

	cctx, cancel := context.WithCancel(p.ctx)
	err := p.sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		// fmt.Println("received data, ", msg.Data)
		output = msg.Data

		msg.Ack()

		cancel() // cancel right after receiving the first message
	})
	handle("Some problem with receiving from sub, ", err)

	return output
}

// Receiving waits for the first message to arrive from the subscription and returns it.
// No more than one message is returned per function call
func (p PSChannel) ReceivingTimed(start time.Time) chan []byte {
	// fmt.Println("Called ReceivingTime function")

	var output chan []byte
	output = make(chan []byte, 100)
	if p.kind == Sending {
		return output // nothing to receive if kind set to send
	}

	var mu sync.Mutex

	// fmt.Println("setting up receive")

	go func() {
		cctx, _ := context.WithCancel(p.ctx)
		err := p.sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
			mu.Lock()
			defer mu.Unlock()

			// fmt.Println("Gotten message")
			msg.Ack()                         // make sure to ack() all received messages!
			if msg.PublishTime.After(start) { // only return messages that were sent after the start time of current unit
				// fmt.Println("received data, ", msg.Data)
				output <- msg.Data

				// cancel() // cancel right after receiving the first message
			}
		})
		handle("Some problem with receiving from sub, ", err)
	}()

	// fmt.Println("Done setting up receive")

	return output
}

// TO MAYBE DO: _if needed_ implement non-blocking subscription handling,
// 				as well as handling messages via a function
