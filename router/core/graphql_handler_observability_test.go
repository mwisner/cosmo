package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wundergraph/cosmo/router/pkg/statistics"
)

func TestSubscriptionResolutionErrorReason(t *testing.T) {
	t.Parallel()
	tests := map[errorType]statistics.SubscriptionResolutionErrorReason{
		errorTypeUnauthorized:        statistics.SubscriptionResolutionErrorAuthorization,
		errorTypeContextCanceled:     statistics.SubscriptionResolutionErrorContextCanceled,
		errorTypeContextTimeout:      statistics.SubscriptionResolutionErrorTimeout,
		errorTypeUpgradeFailed:       statistics.SubscriptionResolutionErrorFetch,
		errorTypeEDFS:                statistics.SubscriptionResolutionErrorFetch,
		errorTypeStreamsHandlerError: statistics.SubscriptionResolutionErrorHandler,
		errorTypeEDFSInvalidMessage:  statistics.SubscriptionResolutionErrorInvalidMessage,
		errorTypeMergeResult:         statistics.SubscriptionResolutionErrorResolve,
		errorTypeUnknown:             statistics.SubscriptionResolutionErrorUnknown,
	}
	for kind, want := range tests {
		require.Equal(t, want, subscriptionResolutionErrorReason(kind))
	}
}
