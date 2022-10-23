package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/joho/godotenv/autoload"
)

var (
	sourceHost          = flag.String("source-host", "localhost", "Source host")
	sourcePort          = flag.Int("source-port", 6379, "Source port")
	destinationHost     = flag.String("destination-host", "localhost", "Destination host")
	destinationPort     = flag.Int("destination-port", 6379, "Destination port")
	sourcePassword      = flag.String("source-password", "", "Source password")
	destinationPassword = flag.String("destination-password", "", "Destination password")
	sourceUsername      = flag.String("source-username", "", "Source username")
	destinationUsername = flag.String("destination-username", "", "Destination username")
	sourceDatabase      = flag.Int("source-database", 0, "Source database")
	destinationDatabase = flag.Int("destination-database", 0, "Destination database")
	dumpData            = flag.String("dump-to-file", "", "Dump data to a file")
	skipKey             = flag.Int("skip", 0, "Skip to key")
)

func loadFromEnv() {
	if v := os.Getenv("SOURCE_HOST"); v != "" && *sourceHost == "localhost" {
		sourceHost = &v
	}
	if v := os.Getenv("SOURCE_PORT"); v != "" && *sourcePort == 6379 {
		i, _ := strconv.Atoi(v)
		sourcePort = &i
	}
	if v := os.Getenv("SOURCE_PASSWORD"); v != "" && *sourcePassword == "" {
		sourcePassword = &v
	}
	if v := os.Getenv("SOURCE_USERNAME"); v != "" && *sourceUsername == "" {
		sourceUsername = &v
	}
	if v := os.Getenv("SOURCE_DATABASE"); v != "" && *sourceDatabase == 0 {
		i, _ := strconv.Atoi(v)
		sourceDatabase = &i
	}
	if v := os.Getenv("DESTINATION_HOST"); v != "" && *destinationHost == "localhost" {
		destinationHost = &v
	}
	if v := os.Getenv("DESTINATION_PORT"); v != "" && *destinationPort == 6379 {
		i, _ := strconv.Atoi(v)
		destinationPort = &i
	}
	if v := os.Getenv("DESTINATION_PASSWORD"); v != "" && *destinationPassword == "" {
		destinationPassword = &v
	}
	if v := os.Getenv("DESTINATION_USERNAME"); v != "" && *destinationUsername == "" {
		destinationUsername = &v
	}
	if v := os.Getenv("DESTINATION_DATABASE"); v != "" && *destinationDatabase == 0 {
		i, _ := strconv.Atoi(v)
		destinationDatabase = &i
	}
}

func writeToFile(w io.Writer, key, value string, ttl time.Duration) {
	if w == nil {
		return
	}
	w.Write([]byte(key + "\n"))
	if ttl <= 0 {
		w.Write([]byte("0\n"))
	} else {
		w.Write([]byte(strconv.FormatInt(ttl.Milliseconds()*1000, 10) + "\n"))
	}
	w.Write([]byte(base64.RawStdEncoding.EncodeToString([]byte(value)) + "\n"))
}

func main() {
	ctx := context.Background()
	flag.Parse()
	loadFromEnv()

	srcConn := redis.NewClient(&redis.Options{
		Addr:        *sourceHost + ":" + strconv.Itoa(*sourcePort),
		Username:    *sourceUsername,
		Password:    *sourcePassword,
		DB:          *sourceDatabase,
		ReadTimeout: time.Minute,
	})
	dstConn := redis.NewClient(&redis.Options{
		Addr:         *destinationHost + ":" + strconv.Itoa(*destinationPort),
		Username:     *destinationUsername,
		Password:     *destinationPassword,
		DB:           *destinationDatabase,
		WriteTimeout: time.Minute,
	})

	if err := srcConn.Ping(ctx).Err(); err != nil {
		fmt.Println("Source connection error:", err)
		flag.Usage()
		os.Exit(1)
	}
	if err := dstConn.Ping(ctx).Err(); err != nil {
		fmt.Println("Source connection error:", err)
		flag.Usage()
		os.Exit(1)
	}
	fmt.Println("CONNECTED TO SOURCE AND DESTINATION")

	allKeys, err := srcConn.Keys(ctx, "*").Result()
	if err != nil {
		fmt.Println("Error getting keys:", err)
		os.Exit(1)
	}
	fmt.Println("GOT ALL KEYS:", len(allKeys))
	// os.WriteFile("keys.txt", []byte(fmt.Sprintf("%v", allKeys)), 0644)

	var dkeys *os.File
	if *dumpData != "" {
		dkeys, err = os.Create(*dumpData)
		if err != nil {
			fmt.Println("Error creating file:", err)
			os.Exit(1)
		}
		defer dkeys.Close()
	}

	for i, key := range allKeys {
		if i < *skipKey {
			continue
		}
		retryCount := 0
	retryKey:
		dur, err := srcConn.PTTL(ctx, key).Result()
		if err != nil {
			fmt.Println("Error getting TTL:", key, err)
			if retryCount < 3 {
				goto retryKey
			} else {
				os.Exit(1)
			}
		}
		data, err := srcConn.Dump(ctx, key).Result()
		if err != nil {
			fmt.Println("Error dumping key:", key, err)
			if err == redis.Nil {
				continue
			}
			retryCount++
			if retryCount < 3 {
				goto retryKey
			} else {
				os.Exit(1)
			}
		}
		writeToFile(dkeys, key, data, dur)
		if err := dstConn.Restore(ctx, key, dur, data).Err(); err != nil {
			if strings.Contains(err.Error(), "BUSYKEY") {
				fmt.Println("Key already exists:", key)
				continue
			}
			fmt.Println("Error restoring key:", err)
			os.Exit(1)
		}
		fmt.Println("COPIED KEY", fmt.Sprintf("%d/%d", i+1, len(allKeys)), key)
	}
	fmt.Println("COPIED ALL KEYS")
}
