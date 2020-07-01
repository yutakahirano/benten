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

	"github.com/fsnotify/fsnotify"
	"github.com/yutakahirano/benten"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/datastore"
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

func updateMetadata(ctx context.Context, client *datastore.Client, metadata *benten.Metadata) error {
	tr, err := client.NewTransaction(ctx)
	if err != nil {
		logger.Printf("Failed to create a transaction: %v\n", err)
		return err
	}
	defer tr.Rollback()

	// Delete existing entries having the same content hash.
	query := datastore.NewQuery(benten.PieceKind).Transaction(tr).Filter("Hash =", metadata.Hash)
	t := client.Run(ctx, query)
	for {
		var existingKey *datastore.Key
		existingKey, err = t.Next(nil)
		if err != nil {
			break
		}
		err = tr.Delete(existingKey)
		if err != nil {
			break
		}
	}
	if err != iterator.Done {
		logger.Printf("Failed to delete existing metadata: %v\n", err)
		return err
	}
	// Delete existing entries having the same path.
	query = datastore.NewQuery(benten.PieceKind).Transaction(tr).Filter("Path =", metadata.Path)
	t = client.Run(ctx, query)
	for {
		var existingKey *datastore.Key
		existingKey, err = t.Next(nil)
		if err != nil {
			break
		}
		err = tr.Delete(existingKey)
		if err != nil {
			break
		}
	}
	if err != iterator.Done {
		logger.Printf("Failed to delete existing metadata: %v\n", err)
		return err
	}

	key := datastore.IncompleteKey(benten.PieceKind, nil)
	_, err = tr.Put(key, metadata)
	if err != nil {
		logger.Printf("Failed to put %v: %v\n", *key, err)
		return err
	}
	_, err = tr.Commit()
	if err != nil {
		logger.Printf("Failed to commit the transaction: %v\n", err)
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
	var configFileName string
	flag.StringVar(&configFileName, "config", "", "config file name")
	flag.BoolVar(&full, "full", false, "full")

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

	logger.Printf("ProjectID = %s\n", config.ProjectID)
	logger.Printf("BucketName = %s\n", config.BucketName)
	logger.Printf("ServiceAccountKey = %s\n", config.ServiceAccountKey)
	logger.Printf("Target = %s\n", config.Target)

	projectID = config.ProjectID
	bucketName = config.BucketName
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", config.ServiceAccountKey)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Fatalf("Failed to create a Watcher: %v\n", err)
	}

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
