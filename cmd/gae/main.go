package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/storage"
	"github.com/yutakahirano/benten"
	"google.golang.org/api/iterator"
)

var projectID string

func respond(w http.ResponseWriter, code int, message string) {
	if code/100 != 2 {
		log.Print(message)
	}
	w.WriteHeader(code)
	_, err := w.Write([]byte(message))
	if err != nil {
		// 500 internal server error
		w.WriteHeader(500)
	}
}

func get(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("name")
	bucketName := q.Get("bucket")

	deadline := 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	client, err := storage.NewClient(ctx)
	if err != nil {
		respond(w, 500, fmt.Sprintf("Failed to create client: %v", err))
		return
	}
	bucket := client.Bucket(bucketName)

	object := bucket.Object(name)
	attrs, err := object.Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		respond(w, 404, fmt.Sprintf("Not found: %s", name))
		return
	}
	if err != nil {
		respond(w, 500, fmt.Sprintf("Failed to get attrs: %v", err))
		return
	}
	reader, err := object.NewReader(ctx)

	if err != nil {
		respond(w, 500, fmt.Sprintf("Failed to get reader: %v", err))
		return
	}
	w.WriteHeader(200)
	w.Header().Add("content-type", attrs.ContentType)
	_, err = io.Copy(w, reader)
	if err != nil {
		log.Printf("Failed to write data to response: %v", err)
	}
}

func list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	search := benten.Normalize(q.Get("search"))
	var err error
	limit := 10
	limitString := q.Get("limit")
	if limitString != "" {
		limit, err = strconv.Atoi(limitString)

		if err != nil {
			respond(w, 400, fmt.Sprintf("limit (%v) is not a valid number", limitString))
			return
		}
		if limit < 0 || limit > 1000*1000 {
			respond(w, 400, fmt.Sprintf("limit (%v) is out of range", limit))
			return
		}
	}
	if len(search) < benten.GramSizeForAscii {
		respond(w, 400, fmt.Sprintf("The query is too small"))
		return
	}
	isASCII := true
	for i := 0; i < benten.GramSizeForAscii; i++ {
		isASCII = isASCII && search[i] <= unicode.MaxASCII
	}
	var bytes []byte
	if isASCII {
		bytes = []byte(search[0:benten.GramSizeForAscii])
	} else if len(search) < benten.GramSizeForNonAscii {
		respond(w, 404, fmt.Sprintf("The query is too small"))
		return
	} else {
		bytes = []byte(search[0:benten.GramSizeForNonAscii])
	}

	ctx := context.Background()
	deadline := 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	client, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		respond(w, 500, fmt.Sprintf("Failed to create a datastore client: %v", err))
		return
	}

	query := datastore.NewQuery(benten.PieceIndexKind).Filter("Key =", bytes).Order("Value").Limit(limit)
	t := client.Run(ctx, query)
	pieces := make([]benten.Metadata, 0)
	var lastKey *datastore.Key = nil
	for {
		var index benten.PieceIndex
		_, err := t.Next(&index)
		if err == iterator.Done {
			break
		}
		if err != nil {
			respond(w, 500, fmt.Sprintf("Failed to get key: %v", err))
			return
		}
		if lastKey != nil && index.Value.ID == lastKey.ID {
			continue
		}
		lastKey = index.Value
		var piece benten.Metadata
		err = client.Get(ctx, index.Value, &piece)
		if err != nil {
			respond(w, 500, fmt.Sprintf("Failed to get metadata: %v", err))
			return
		}
		if strings.Contains(strings.ToLower(piece.Title), search) ||
			strings.Contains(strings.ToLower(piece.Album), search) ||
			strings.Contains(strings.ToLower(piece.Artist), search) ||
			strings.Contains(strings.ToLower(piece.AlbumArtist), search) {
			pieces = append(pieces, piece)
		}
	}
	w.WriteHeader(200)
	w.Header().Add("content-type", "application/json")
	json.NewEncoder(w).Encode(pieces)
}

func handle(w http.ResponseWriter, r *http.Request) {
	log.Printf("request: %s", r.URL)

	if r.URL.Path == "/api/get" {
		get(w, r)
		return
	}
	if r.URL.Path == "/api/list" {
		list(w, r)
		return
	}

	w.WriteHeader(404)
	w.Header().Add("content-type", "text/plain")
	w.Write([]byte("Not Found"))
}

func main() {
	http.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		handle(w, r)
	})

	port := os.Getenv("PORT")
	projectID = os.Getenv("GOOGLE_CLOUD_PROJECT")

	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}

	log.Printf("Listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed : %v", err)
	}
}
