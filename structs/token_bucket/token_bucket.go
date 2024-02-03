package token_bucket

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"
)

// TokenBucket structure
type TokenBucket struct {
	rate                int64
	maxTokens           int64
	currentTokens       int64
	lastRefillTimestamp time.Time
}

// Default Constructor
func NewTokenBucket(rate, maxTokens int64) *TokenBucket {
	return &TokenBucket{
		rate:                rate,
		maxTokens:           maxTokens,
		currentTokens:       maxTokens,
		lastRefillTimestamp: time.Now(),
	}
}

// Function used to refill token bucket
func (tb *TokenBucket) refill() {
	now := time.Now()
	end := time.Since(tb.lastRefillTimestamp)
	tokensTobeAdded := int64(end.Seconds()) * tb.rate
	tb.currentTokens = int64(math.Min(float64(tokensTobeAdded+tb.currentTokens), float64(tb.maxTokens)))
	if tokensTobeAdded != 0 {
		tb.lastRefillTimestamp = now
	}
}

func (tb *TokenBucket) IsRequestAllowed(tokens int64) (string, bool) {
	tb.refill()
	if tb.currentTokens >= tokens {
		tb.currentTokens -= tokens
		tb.AppendRequest("token_bucket/requests.bin", []byte(time.Now().Format("15:04:05")+", "+strconv.Itoa(int(tokens))+", ALLOWED\n"))
		return "Zahtev dozoljen", true
	}
	tb.AppendRequest("token_bucket/requests.bin", []byte(time.Now().Format("15:04:05")+", "+strconv.Itoa(int(tokens))+", BLOCKED\n"))
	return "Zahtev nije dozvoljen", false
}

// SerializeTokenBucket serializes the TokenBucket
func (tb *TokenBucket) SerializeTokenBucket() ([]byte, error) {
	buffer := new(bytes.Buffer)

	// Write rate, maxTokens, currentTokens, and lastRefillTimestamp to the buffer
	binary.Write(buffer, binary.LittleEndian, tb.rate)
	binary.Write(buffer, binary.LittleEndian, tb.maxTokens)
	binary.Write(buffer, binary.LittleEndian, tb.currentTokens)
	binary.Write(buffer, binary.LittleEndian, tb.lastRefillTimestamp.UnixNano())

	return buffer.Bytes(), nil
}

// DeserializeTokenBucket deserializes the TokenBucket
func DeserializeTokenBucket(data []byte) (*TokenBucket, error) {
	buffer := bytes.NewReader(data)

	var rate, maxTokens, currentTokens int64
	var lastRefillTimestampNano int64

	// Read rate, maxTokens, currentTokens, and lastRefillTimestamp from the buffer
	err := binary.Read(buffer, binary.LittleEndian, &rate)
	if err != nil {
		return nil, err
	}

	err = binary.Read(buffer, binary.LittleEndian, &maxTokens)
	if err != nil {
		return nil, err
	}

	err = binary.Read(buffer, binary.LittleEndian, &currentTokens)
	if err != nil {
		return nil, err
	}

	err = binary.Read(buffer, binary.LittleEndian, &lastRefillTimestampNano)
	if err != nil {
		return nil, err
	}

	lastRefillTimestamp := time.Unix(0, lastRefillTimestampNano)

	return &TokenBucket{
		rate:                rate,
		maxTokens:           maxTokens,
		currentTokens:       currentTokens,
		lastRefillTimestamp: lastRefillTimestamp,
	}, nil
}

// AppendRequest appends binary data to a requests.bin file
func (tb *TokenBucket) AppendRequest(filename string, data []byte) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	return err
}

// InitRequestsFile creates empty file for requests
func (tb *TokenBucket) InitRequestsFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	return nil
}

// ToString returns a string representation of the TokenBucket
func (tb *TokenBucket) ToString() string {
	return fmt.Sprintf("Rate: %d, MaxTokens: %d, CurrentTokens: %d, LastRefillTimestamp: %v",
		tb.rate, tb.maxTokens, tb.currentTokens, tb.lastRefillTimestamp.Format("15:04:05"))
}

//func main() {
//
//	tb := NewTokenBucket(3, 10)
//	err := tb.InitRequestsFile("token_bucket/requests.bin")
//
//	if err != nil {
//		fmt.Println("Unsuccessful initialization of the requests.bin file!")
//		return
//	}
//
//	for i := 1; i <= 15; i++ {
//		fmt.Println(tb.IsRequestAllowed(2), " at ", time.Now().Format("15:04:05"))
//		time.Sleep(300 * time.Millisecond)
//	}
//
//	data, _ := tb.SerializeTokenBucket()
//
//	tb2, _ := DeserializeTokenBucket(data)
//
//	fmt.Println((*tb2).ToString())
//}
