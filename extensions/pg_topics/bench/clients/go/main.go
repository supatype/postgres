package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func check(err error) {
	if err != nil {
		fmt.Println("error", err)
		os.Exit(1)
	}
}

func main() {
	cmd, topic := os.Args[1], os.Args[2]
	pem, err := os.ReadFile("/w/server.crt")
	check(err)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	opts := []kgo.Opt{
		kgo.SeedBrokers(os.Getenv("BOOTSTRAP")),
		kgo.DialTLSConfig(&tls.Config{RootCAs: pool}),
		kgo.SASL(plain.Auth{User: os.Getenv("KAFKA_USER"), Pass: os.Getenv("KAFKA_PASSWORD")}.AsMechanism()),
	}
	if os.Getenv("DEBUG") != "" {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	}
	ctx := context.Background()

	if cmd == "produce" {
		cl, err := kgo.NewClient(opts...)
		check(err)
		for b := 0; b < 1000; b += 100 {
			var records []*kgo.Record
			for i := b; i < b+100; i++ {
				records = append(records, &kgo.Record{
					Topic:   topic,
					Key:     []byte("k-" + strconv.Itoa(i)),
					Value:   []byte(`{"i": ` + strconv.Itoa(i) + `}`),
					Headers: []kgo.RecordHeader{{Key: "n", Value: []byte(strconv.Itoa(i))}},
				})
			}
			check(cl.ProduceSync(ctx, records...).FirstErr())
		}
		cl.Close()
		fmt.Println("acked 1000")
		return
	}

	group, name := os.Args[3], os.Args[4]
	cl, err := kgo.NewClient(append(opts, kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
		kgo.AutoCommitInterval(500*time.Millisecond))...)
	check(err)
	for {
		if _, err := os.Stat("/w/stop-" + name); err == nil {
			break
		}
		poll, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		fetches := cl.PollFetches(poll)
		cancel()
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				fmt.Println("error", t, p, err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) { fmt.Println("msg", r.Partition, r.Offset) })
	}
	check(cl.CommitUncommittedOffsets(ctx))
	cl.Close()
	fmt.Println("closed")
}
