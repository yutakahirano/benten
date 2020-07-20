package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/storage"
	"github.com/yutakahirano/benten"
	"google.golang.org/api/iterator"
)

// Response hogefuga
type Response struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

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

	bs := [4096]byte{}
	for {
		read, err := reader.Read(bs[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Failed to read data from response: %v", err)
			return
		}
		offset := 0
		for offset < read {
			written, err := w.Write(bs[offset:read])
			if err != nil {
				log.Printf("Failed to write data to response: %v", err)
				return
			}
			offset += written
		}
	}
}

func list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	search := strings.ToLower(q.Get("search"))

	if len(search) < benten.GramSizeForAscii {
		respond(w, 404, fmt.Sprintf("The query is too small"))
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
	client, err := datastore.NewClient(ctx, "")
	if err != nil {
		respond(w, 500, fmt.Sprintf("Failed to create a datastore client: %v", err))
		return
	}

	query := datastore.NewQuery(benten.PieceIndexKind).Filter("Key =", bytes).Order("Value")
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

func handle(writer http.ResponseWriter, request *http.Request) {
	log.Printf("request: %s", request.URL)

	if request.URL.Path == "/api/get" {
		get(writer, request)
		return
	}
	if request.URL.Path == "/api/list" {
		list(writer, request)
		return
	}

	json.NewEncoder(writer).Encode(Response{Status: "ok", Message: "Hello"})
}

func main() {
	http.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		handle(w, r)
	})

	datastoreProjectIDName := "DATASTORE_PROJECT_ID"
	port := os.Getenv("PORT")
	if os.Getenv(datastoreProjectIDName) == "" {
		projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
		log.Printf("%s is not set, so set it to %s\n", datastoreProjectIDName, projectID)
		os.Setenv(datastoreProjectIDName, projectID)
	}

	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}

	log.Printf("Listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed : %v", err)
	}
}
