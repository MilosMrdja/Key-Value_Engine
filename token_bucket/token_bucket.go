package main

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
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
	data, _ := tb.Deserialize()
	for _, value := range data {
		fmt.Println(value)
	}
	data = append(data, Log{Hours: time.Now().Hour(), Minutes: time.Now().Minute(), Seconds: time.Now().Second(), Value: tokens})
	serializedData, _ := tb.Serialize(data)
	tb.WriteToFile("requests.bin", serializedData)
	if tb.currentTokens >= tokens {
		tb.currentTokens -= tokens
		return "Request Allowed"
	}
	return "Request Blocked"
}

type Log struct {
	Hours   int
	Minutes int
	Seconds int
	Value   int64
}

func (tb *TokenBucket) Serialize(data []Log) ([]byte, error) {
	var lines []string

	for _, entry := range data {
		line := fmt.Sprintf("%02d:%02d:%02d,%d", entry.Hours, entry.Minutes, entry.Seconds, entry.Value)
		lines = append(lines, line)
	}

	return []byte(strings.Join(lines, "\n")), nil
}

func (tb *TokenBucket) WriteToFile(filename string, data []byte) error {
	return ioutil.WriteFile(filename, data, 0644)
}

func (tb *TokenBucket) Deserialize() ([]Log, error) {

	file, err := os.Open("requests.bin")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	readData := make([]byte, fileSize)

	_, err = file.Read(readData)
	if err != nil {
		return nil, err
	}

	var result []Log

	dataStr := string(readData)

	// Split lines
	lines := strings.Split(dataStr, "\n")

	for _, line := range lines {
		// Split each line into hours:minutes,value
		parts := strings.Split(line, ",")

		// Extract hours, minutes, and value from the split parts
		timeParts := strings.Split(parts[0], ":")
		hours, err := strconv.Atoi(timeParts[0])
		if err != nil {
			return nil, err
		}

		minutes, err := strconv.Atoi(timeParts[1])
		if err != nil {
			return nil, err
		}

		seconds, err := strconv.Atoi(timeParts[2])
		if err != nil {
			return nil, err
		}

		value, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, err
		}

		// Create a MyData instance and add it to the result
		result = append(result, Log{Hours: hours, Minutes: minutes, Seconds: seconds, Value: int64(value)})
	}

	return result, nil
}

func main() {
	tb := NewTokenBucket(3, 10)

	for i := 1; i <= 30; i++ {
		fmt.Println(i, tb.IsRequestAllowed(4), " at ", time.Now().Format("15:04:05"))
		time.Sleep(1000 * time.Millisecond)
	}
}
