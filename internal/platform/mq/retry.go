package mq

// Implemented in lesson 3.

// RetryTier picks the delay tier for an invoice event whose handler has failed
// attempt times in a row, returning the queue-name suffix, or ok=false once the
// event should be parked for an operator.
//
// Invariant: a failing event waits longer on each attempt and is never retried
// without a delay; after the last tier it is parked, never dropped.
//
// Without the staircase, one stuck invoice would retry in a hot loop and block
// its queue, or be dropped and never delivered or settled.
func RetryTier(attempt int) (string, bool) { return "", false }

// retryTierImpl lets tests swap the tier policy.
var retryTierImpl = RetryTier
