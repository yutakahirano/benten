package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/fsnotify/fsnotify"
	"github.com/yutakahirano/benten"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/dhowden/tag"
)

var logger *log.Logger

func addToWatcher(watcher *fsnotify.Watcher, path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if info.Mode().IsDir() {
		logger.Printf("Add %s to watcher\n", path)
		return watcher.Add(path)
	}
	return nil
}

func addToWatcherRecursively(watcher *fsnotify.Watcher, path string) error {
	return filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		return addToWatcher(watcher, path, info, err)
	})
}

var projectID string
var bucketName string
var subscriptionID string

// Uploads `picture` into `bucket`, with `key`.
func uploadPicture(ctx context.Context, bucket *storage.BucketHandle, key string, picture *tag.Picture) error {
	object := bucket.Object(key)
	writer := object.NewWriter(ctx)
	_, err := io.Copy(writer, bytes.NewBuffer(picture.Data))
	if err != nil {
		logger.Printf("Failed to copy bytes: %v\n", err)
		return err
	}
	err = writer.Close()
	if err != nil {
		logger.Printf("Failed to close the writer: %v\n", err)
		return err
	}
	_, err = object.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: picture.MIMEType})
	if err != nil {
		logger.Printf("Failed to update object's attributes: %v\n", err)
		return err
	}
	return err
}

func getAlbumArtFromDir(dir string) (*tag.Picture, error) {
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var largestArt os.FileInfo = nil
	largestArtType := ""
	albumArtPattern := regexp.MustCompile("(?i)^AlbumArt.*\\.(jpg|png)$")
	for _, fileInfo := range fileInfos {
		if match := albumArtPattern.FindStringSubmatch(fileInfo.Name()); match != nil {
			if largestArt == nil || largestArt.Size() < fileInfo.Size() {
				largestArt = fileInfo
				if strings.ToLower(match[1]) == "jpg" {
					largestArtType = "image/jpeg"
				} else if strings.ToLower(match[1]) == "png" {
					largestArtType = "image/png"
				} else {
					panic("notreached")
				}
			}
		}
	}
	if largestArt == nil {
		return nil, nil
	}
	file, err := os.Open(largestArt.Name())
	defer file.Close()
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	var bs [4096]byte
	for {
		n, err := file.Read(bs[:])
		buffer.Write(bs[0:n])
		if err == io.EOF {
			return &tag.Picture{
				MIMEType: largestArtType,
				Data:     buffer.Bytes(),
			}, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func generateWordsForIndexInternal(text string, words *map[string]struct{}) {
	if len(text) < benten.GramSizeForAscii {
		return
	}
	for i := 0; i <= len(text)-benten.GramSizeForAscii; i++ {
		isASCII := true
		for j := 0; j <= benten.GramSizeForNonAscii; j++ {
			if (j == benten.GramSizeForAscii && isASCII) ||
				j == benten.GramSizeForNonAscii {
				(*words)[text[i:i+j]] = struct{}{}
				break
			}
			if i+j == len(text) {
				break
			}
			isASCII = isASCII && text[i+j] <= unicode.MaxASCII
		}
	}
}

func generateWordsForIndex(text string, words *map[string]struct{}) {
	generateWordsForIndexInternal(benten.Normalize(text), words)
}

func spanPieceIndex(ctx context.Context, client *datastore.Client, metadata *benten.Metadata, key *datastore.Key) error {
	words := make(map[string]struct{})
	generateWordsForIndex(strings.ToLower(metadata.Title), &words)
	generateWordsForIndex(strings.ToLower(metadata.Album), &words)
	generateWordsForIndex(strings.ToLower(metadata.Artist), &words)
	generateWordsForIndex(strings.ToLower(metadata.AlbumArtist), &words)
	generateWordsForIndex(strings.ToLower(metadata.Composer), &words)

	tr, err := client.NewTransaction(ctx)
	if err != nil {
		return err
	}
	defer tr.Rollback()

	var entry benten.PieceIndex
	entry.Value = key
	for word := range words {
		entry.Key = []byte(word)
		_, err := tr.Put(datastore.IncompleteKey(benten.PieceIndexKind, nil), &entry)
		if err != nil {
			return err
		}
	}

	_, err = tr.Commit()
	return err
}

func clearIndex() error {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		return err
	}

	query := datastore.NewQuery(benten.PieceIndexKind)
	iter := client.Run(ctx, query)
	for {
		key, err := iter.Next(nil)
		if err != nil {
			if err == iterator.Done {
				return nil
			}
			return err
		}
		err = client.Delete(ctx, key)
		if err != nil {
			return err
		}
	}
}

func deleteIndexFor(ctx context.Context, client *datastore.Client, tr *datastore.Transaction, keys []*datastore.Key) error {
	for key := range keys {
		query := datastore.NewQuery(benten.PieceIndexKind).Transaction(tr).Filter("Value =", key)
		t := client.Run(ctx, query)
		for {
			existingKey, err := t.Next(nil)
			if err != nil {
				if err == iterator.Done {
					break
				}
				return err
			}
			err = tr.Delete(existingKey)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func deleteMatchedPieces(iter *datastore.Iterator, tr *datastore.Transaction) ([](*datastore.Key), error) {
	deletedPieces := make([]*datastore.Key, 0)
	for {
		key, err := iter.Next(nil)
		if err != nil {
			if err == iterator.Done {
				return deletedPieces, nil
			}
			return deletedPieces, err
		}
		err = tr.Delete(key)
		if err != nil {
			return deletedPieces, err
		}
		deletedPieces = append(deletedPieces, key)
	}
}

func updateMetadata(ctx context.Context, client *datastore.Client, metadata *benten.Metadata) error {
	tr, err := client.NewTransaction(ctx)
	if err != nil {
		logger.Printf("Failed to create a transaction: %v\n", err)
		return err
	}
	defer tr.Rollback()

	// Delete existing entries having the same content hash.
	query := datastore.NewQuery(benten.PieceKind).Transaction(tr).Filter("Hash =", metadata.Hash)
	deletedPieces, err := deleteMatchedPieces(client.Run(ctx, query), tr)
	if err != nil {
		logger.Printf("Failed to delete existing metadata: %v\n", err)
		return err
	}
	// Delete existing entries having the same path.
	query = datastore.NewQuery(benten.PieceKind).Transaction(tr).Filter("Path =", metadata.Path)
	deletedPieces2, err := deleteMatchedPieces(client.Run(ctx, query), tr)
	if err != nil {
		logger.Printf("Failed to delete existing metadata: %v\n", err)
		return err
	}
	deletedPieces = append(deletedPieces, deletedPieces2...)
	err = deleteIndexFor(ctx, client, tr, deletedPieces)
	if err != nil {
		return err
	}

	incompleteKey := datastore.IncompleteKey(benten.PieceKind, nil)
	pendingKey, err := tr.Put(incompleteKey, metadata)
	if err != nil {
		logger.Printf("Failed to put %v: %v\n", *incompleteKey, err)
		return err
	}
	commit, err := tr.Commit()
	if err != nil {
		logger.Printf("Failed to commit the transaction: %v\n", err)
	}

	err = spanPieceIndex(ctx, client, metadata, commit.Key(pendingKey))
	if err != nil {
		logger.Printf("Failed to update title index: %v", err)
		return err
	}
	return err
}

func syncInternal(ch chan string) {
	// A collection of album pictures. Each of key is either
	//  - the path of the dictionary that the album is contined, or
	//  - the base64 encoded hash value of the bytes representing the album picture.
	// Either way, the value is the base64 encoded hash value of the bytes representing the album picture.
	albumPictures := make(map[string]string)

	ctx := context.Background()
	datastoreClient, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		logger.Printf("Failed to create a datastore client: %v\n", err)
		return
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		logger.Printf("Failed to create a storage client: %v\n", err)
		return
	}
	bucket := client.Bucket(benten.AlbumPictureBucket)
	for {
		filename := <-ch
		fi, err := os.Stat(filename)
		if err != nil {
			logger.Printf("Failed to get stat for %s: %v\n", filename, err)
		}
		if fi.IsDir() {
			continue
		}

		file, err := os.Open(filename)
		if err != nil {
			logger.Printf("Failed to open %s: %v\n", filename, err)
			continue
		}
		defer file.Close()

		logger.Printf("Processing %s...\n", file.Name())
		m, err := tag.ReadFrom(file)
		if err != nil {
			logger.Printf("Failed read tag from %s: %v\n", file.Name(), err)
			continue
		}
		hash, err := tag.Sum(file)
		if err != nil {
			logger.Printf("Failed calculate the sum from %s: %v\n", file.Name(), err)
			continue
		}

		pictureHash := ""
		if m.Picture() == nil {
			var ok bool
			dirname := filepath.Dir(file.Name())
			pictureHash, ok = albumPictures[dirname]
			if !ok {
				picture, err := getAlbumArtFromDir(dirname)
				if err != nil {
					logger.Printf("Failed to get an album art in %v: %v", dirname, err)
				}
				if picture != nil {
					sum := sha256.Sum256(picture.Data)
					pictureHash = base64.StdEncoding.EncodeToString(sum[:])
					err = uploadPicture(ctx, bucket, pictureHash, picture)
					if err == nil {
						albumPictures[dirname] = pictureHash
						albumPictures[pictureHash] = pictureHash
					}
				}
			}
		}
		if pictureHash == "" && m.Picture() != nil {
			sum := sha256.Sum256(m.Picture().Data)
			pictureHash = base64.StdEncoding.EncodeToString(sum[:])
			_, ok := albumPictures[pictureHash]
			if !ok {
				err = uploadPicture(ctx, bucket, pictureHash, m.Picture())
				if err == nil {
					albumPictures[pictureHash] = pictureHash
				}
			}
		}

		metadata := benten.NewMetadata(m, pictureHash, hash, file.Name())
		err = updateMetadata(ctx, datastoreClient, &metadata)
		if err == nil {
			logger.Printf("Successfully updated data for %s\n", file.Name())
		}
	}
}

func sync(ch chan string) {
	filenames := make(map[string]time.Time)
	chInternal := make(chan string)

	// We don't want to sync files that are being updated, so we wait for a while.
	duration := time.Second * 5
	isTimerActive := false

	go syncInternal(chInternal)

	for {
		filename := <-ch
		if filename == "" {
			// This is called from the timer below.
			isTimerActive = false
			oldFilenames := filenames
			filenames = make(map[string]time.Time)
			for name, timestamp := range oldFilenames {
				if time.Now().Sub(timestamp) >= duration {
					chInternal <- name
				} else {
					filenames[name] = timestamp
				}
			}
		} else {
			filenames[filename] = time.Now()
		}
		if len(filenames) > 0 && !isTimerActive {
			time.AfterFunc(duration*2, func() {
				ch <- ""
			})
			isTimerActive = true
		}
	}
}

func uploadPiece(ctx context.Context, bucket *storage.BucketHandle, key string, path string) error {
	file, err := os.Open(path)
	defer file.Close()
	if err != nil {
		logger.Printf("Failed to open %s: %v", path, err)
		return err
	}
	object := bucket.Object(key)
	writer := object.NewWriter(ctx)
	defer writer.Close()
	_, err = io.Copy(writer, file)
	if err != nil {
		logger.Printf("Failed to copy the contents of %s: %v", path, err)
		return err
	}
	err = writer.Close()
	if err != nil {
		logger.Printf("Failed to copy the contents of %s: %v", path, err)
	}
	return err
}

func uploadContentsInternal(ctx context.Context, m *pubsub.Message) error {
	type Entry = struct {
		Path string
		Key  string
	}
	var entries []Entry
	err := json.Unmarshal(m.Data, &entries)
	if err != nil {
		logger.Printf("Failed to parse the notification message: %v", err)
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		logger.Printf("Failed to create a storage client: %v\n", err)
		return err
	}
	bucket := client.Bucket(benten.PieceBucket)
	for _, entry := range entries {
		err := uploadPiece(ctx, bucket, entry.Key, entry.Path)
		if err != nil {
			return err
		}
		logger.Printf("Uploaded %s from %s", entry.Key, entry.Path)
	}
	return nil
}

func uploadContents() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		logger.Printf("Failed to create a pubsub client: %v", err)
		return
	}
	sub := client.Subscription(subscriptionID)
	for {
		err := sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			err := uploadContentsInternal(ctx, m)
			if err == nil {
				m.Ack()
			}
		})
		if err != nil {
			logger.Printf("Failed to receive message: %v", err)
		}
	}
}

func walk(path string, ch chan string) {
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			logger.Printf("Found: %v\n", path)
			ch <- path
		}
		return nil
	})
	if err != nil {
		logger.Printf("Error during filepath.Wark: %v\n", err)
	}
}

type config struct {
	ProjectID         string
	BucketName        string
	SubscriptionID    string
	LogFileName       string
	ServiceAccountKey string
	Target            string
}

// Calls os.Exit() when an error happens.
func loadConfig(filename string) config {
	var config config
	file, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to open %s: %v", filename, err)
		os.Exit(1)
	}
	defer file.Close()

	var buffer bytes.Buffer
	_, err = io.Copy(&buffer, file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read %s: %v\n", filename, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "config = %s\n\n", buffer.Bytes())
	err = json.Unmarshal(buffer.Bytes(), &config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to parse %s: %v\n", filename, err)
		os.Exit(1)
	}
	return config
}

func main() {
	var full bool
	var clearIndexFlag bool
	var configFileName string
	flag.StringVar(&configFileName, "config", "", "config file name")
	flag.BoolVar(&full, "full", false, "full")
	flag.BoolVar(&clearIndexFlag, "clear-index", false, "clear index")

	flag.Parse()

	config := loadConfig(configFileName)
	fmt.Fprintf(os.Stderr, "config.logFileName = %s\n", config.LogFileName)

	var logFile *os.File = os.Stderr
	if config.LogFileName != "" {
		var err error
		now := time.Now()
		logFileName := fmt.Sprintf("%s-%04d-%02d", config.LogFileName, now.UTC().Year(), now.UTC().Month())
		logFile, err = os.OpenFile(logFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open a log file: %v", err)
			return
		}
		fmt.Fprintf(os.Stderr, "Add log to %s...\n", logFileName)
	}
	logger = log.New(logFile, "", log.Ldate|log.Ltime|log.Lshortfile|log.LUTC|log.Lmsgprefix)

	logger.Printf("\n")
	logger.Printf("Starting up...\n")
	logger.Printf("ProjectID = %s\n", config.ProjectID)
	logger.Printf("BucketName = %s\n", config.BucketName)
	logger.Printf("ServiceAccountKey = %s\n", config.ServiceAccountKey)
	logger.Printf("Target = %s\n", config.Target)

	projectID = config.ProjectID
	bucketName = config.BucketName
	subscriptionID = config.SubscriptionID
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", config.ServiceAccountKey)

	if clearIndexFlag {
		logger.Printf("Clearing index...\n")
		err := clearIndex()
		if err != nil {
			logger.Printf("Failed to clear index: %v\n", err)
		} else {
			logger.Printf("Successfully cleared the index.\n")
		}
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Fatalf("Failed to create a Watcher: %v\n", err)
	}

	go uploadContents()

	ch := make(chan string)
	go func() {
		if full {
			walk(config.Target, ch)
		}
		if err != nil {
			log.Fatal(err)
		}
		defer watcher.Close()
		addToWatcherRecursively(watcher, config.Target)
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					continue
				}
				if event.Op&fsnotify.Write == fsnotify.Write {
					ch <- event.Name
				}
			case err, _ = <-watcher.Errors:
				logger.Printf("%v\n", err)
			}
		}
	}()
	sync(ch)
}
