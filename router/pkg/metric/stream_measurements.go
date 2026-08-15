package metric

import (
	"fmt"

	otelmetric "go.opentelemetry.io/otel/metric"
)

const (
	messagingSentMessages         = "router.streams.sent.messages"
	messagingConsumedMessages     = "router.streams.received.messages"
	messagingDeduplicatedMessages = "router.streams.deduplicated.messages"
)

var (
	messagingSentMessagesOptions = []otelmetric.Int64CounterOption{
		otelmetric.WithDescription("Number of stream sent messages"),
	}
	messagingConsumedMessagesOptions = []otelmetric.Int64CounterOption{
		otelmetric.WithDescription("Number of stream consumed messages"),
	}
	messagingDeduplicatedMessagesOptions = []otelmetric.Int64CounterOption{
		otelmetric.WithDescription("Number of stream messages dropped as duplicates before dispatch"),
	}
)

type eventInstruments struct {
	producedMessages     otelmetric.Int64Counter
	consumedMessages     otelmetric.Int64Counter
	deduplicatedMessages otelmetric.Int64Counter
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

	deduplicatedCounter, err := meter.Int64Counter(
		messagingDeduplicatedMessages,
		messagingDeduplicatedMessagesOptions...,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create deduplicated messages counter: %w", err)
	}

	return &eventInstruments{
		producedMessages:     producedCounter,
		consumedMessages:     consumedCounter,
		deduplicatedMessages: deduplicatedCounter,
	}, nil
}
