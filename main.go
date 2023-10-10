package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var projectID = "casting-one-dev"

func init_() {
	// client生成時点でemulator設定しておく必要がある
	// os.Setenv("PUBSUB_EMULATOR_HOST", "0.0.0.0:8085")
	cli, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	testPubSubs := []struct {
		topicName        string
		subscriptionName string
		pushEndPoint     string
	}{
		{
			topicName:        "emulator-topic",
			subscriptionName: "emulator-sub",
			pushEndPoint:     "http://127.0.0.1:8080/receive",
		},
	}

	for _, p := range testPubSubs {
		if err := cli.Topic(p.topicName).Delete(ctx); err != nil {
			// 削除済は無視
			if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
				panic(err)
			}
		}
		if err := cli.Subscription(p.subscriptionName).Delete(ctx); err != nil {
			// 削除済は無視
			if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
				panic(err)
			}
		}

		topic, err := cli.CreateTopic(ctx, p.topicName)
		if err != nil {
			panic(err)
		}
		_, err = cli.CreateSubscription(ctx, p.subscriptionName, pubsub.SubscriptionConfig{
			Topic: topic,
			// PushConfig: pubsub.PushConfig{
			//   Endpoint: p.pushEndPoint,
			// },
		})
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	http.HandleFunc("/publish", publishHandler)
	http.HandleFunc("/receive", receiveHandler)
	http.HandleFunc("/pull", pullHandler)

	if err := http.ListenAndServe("127.0.0.1:8080", nil); err != nil {
		panic(err)
	}
}

func publishHandler(w http.ResponseWriter, r *http.Request) {
	// config := &pubsub.ClientConfig{
	//   PublisherCallOptions: &vkit.PublisherCallOptions{
	//     Publish: []gax.CallOption{
	//       gax.WithRetry(func() gax.Retryer {
	//         return gax.OnCodes([]codes.Code{
	//           codes.Aborted,
	//           codes.Canceled,
	//           codes.Internal,
	//           codes.ResourceExhausted,
	//           codes.Unknown,
	//           codes.Unavailable,
	//           codes.DeadlineExceeded,
	//         }, gax.Backoff{
	//           Initial:    , // default 100 milliseconds
	//           Max:        60 * time.Second, // default 60 seconds
	//           Multiplier: 1.45,             // default 1.3
	//         })
	//       }),
	//     },
	//   },
	// }

	// client, err := pubsub.NewClientWithConfig(context.Background(), projectID, config)
	client, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		panic(err)
	}
	res := client.Topic("takashabe-retry-test").Publish(context.Background(), &pubsub.Message{
		Data: []byte("test"),
	})
	id, err := res.Get(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("published: ", id)

	w.WriteHeader(200)
	// fmt.Fprintf(w, "published: %s", id)
	return
}

func pullHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("called pullHandler")

	client, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		panic(err)
	}
	sub := client.Subscription("emulator-sub")
	err = sub.Receive(context.Background(), func(ctx context.Context, msg *pubsub.Message) {
		fmt.Println("received: ", msg.ID)
		msg.Nack()
	})
	if err != nil {
		panic(err)
	}
	return
}

func receiveHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("called receiveHandler: ", time.Now().String())

	b, err := io.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println("body: ", string(b))

	// var msg pubsub.Message
	// if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
	//   panic(err)
	// }
	// fmt.Println("message: ", msg)

	w.WriteHeader(500)
	return
}
