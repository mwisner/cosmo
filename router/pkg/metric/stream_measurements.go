package metric

import (
	"fmt"

	otelmetric "go.opentelemetry.io/otel/metric"
)

const (
	messagingSentMessages      = "router.streams.sent.messages"
	messagingConsumedMessages  = "router.streams.received.messages"
	messagingProcessedMessages = "router.streams.processed.messages"
)

var (
	messagingSentMessagesOptions = []otelmetric.Int64CounterOption{
		otelmetric.WithDescription("Number of stream sent messages"),
	}
	messagingConsumedMessagesOptions = []otelmetric.Int64CounterOption{
		otelmetric.WithDescription("Number of stream consumed messages"),
	}
	messagingProcessedMessagesOptions = []otelmetric.Int64CounterOption{
		otelmetric.WithDescription("Number of stream messages dispatched or dropped before subscription fan-out"),
	}
)

type eventInstruments struct {
	producedMessages  otelmetric.Int64Counter
	consumedMessages  otelmetric.Int64Counter
	processedMessages otelmetric.Int64Counter
}

func newStreamEventInstruments(meter otelmetric.Meter) (*eventInstruments, error) {
	producedCounter, err := meter.Int64Counter(
		messagingSentMessages,
		messagingSentMessagesOptions...,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create sent messages counter: %w", err)
	}

	consumedCounter, err := meter.Int64Counter(
		messagingConsumedMessages,
		messagingConsumedMessagesOptions...,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create received messages counter: %w", err)
	}

	processedCounter, err := meter.Int64Counter(
		messagingProcessedMessages,
		messagingProcessedMessagesOptions...,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create processed messages counter: %w", err)
	}

	return &eventInstruments{
		producedMessages:  producedCounter,
		consumedMessages:  consumedCounter,
		processedMessages: processedCounter,
	}, nil
}
