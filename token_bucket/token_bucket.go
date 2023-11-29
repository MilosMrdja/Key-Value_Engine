package main

import (
	"fmt"
	"math"
	"time"
)

// TokenBucket struktura
type TokenBucket struct {
	rate                int64
	maxTokens           int64
	currentTokens       int64
	lastRefillTimestamp time.Time
}

func NewTokenBucket(rate, maxTokens int64) *TokenBucket {
	return &TokenBucket{
		rate:                rate,
		maxTokens:           maxTokens,
		currentTokens:       maxTokens,
		lastRefillTimestamp: time.Now(),
	}
}

func (tb *TokenBucket) refill() {
	now := time.Now()
	end := time.Since(tb.lastRefillTimestamp)
	tokensTobeAdded := int64(end.Seconds()) * tb.rate
	fmt.Printf("Number of Added Tokens %d --> ", tokensTobeAdded)
	tb.currentTokens = int64(math.Min(float64(tokensTobeAdded+tb.currentTokens), float64(tb.maxTokens)))
	if tokensTobeAdded != 0 {
		tb.lastRefillTimestamp = now
	}
}

func (tb *TokenBucket) IsRequestAllowed(tokens int64) string {
	tb.refill()
	if tb.currentTokens >= tokens {
		tb.currentTokens -= tokens
		return "Request Allowed"
	}
	return "Request Blocked"
}

func main() {
	tb := NewTokenBucket(3, 10)

	for i := 1; i <= 30; i++ {
		fmt.Println(i, tb.IsRequestAllowed(4), " at ", time.Now().Format("15:04:05"))
		time.Sleep(1 * time.Millisecond)
	}
}
