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

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/storage"
	"github.com/dhowden/tag"
)

var logger *log.Logger

func watch(watcher *fsnotify.Watcher) {
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			log.Println("event:", event)
			if event.Op&fsnotify.Write == fsnotify.Write {
				log.Println("modified file:", event.Name)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Println("error:", err)
		}
	}
}

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
	_, err := object.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: picture.MIMEType})
	if err != nil {
		logger.Printf("Failed to update object's attributes: %v\n", err)
		return err
	}
	_, err = io.Copy(writer, bytes.NewBuffer(picture.Data))
	if err != nil {
		logger.Printf("Failed to copy bytes: %v\n", err)
		return err
	}
	err = writer.Close()
	if err != nil {
		logger.Printf("Failed to close the writer: %v\n", err)
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

func sync(ch chan fsnotify.Event) {
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
		ev := <-ch

		log.Printf("op: %v, name: %v\n", ev.Op, ev.Name)
		switch ev.Op {
		case fsnotify.Create:
			file, err := os.Open(ev.Name)
			if err != nil {
				continue
			}
			m, err := tag.ReadFrom(file)
			if err != nil {
				log.Printf("tag.ReadFrom: %v\n", err)
				continue
			}
			hash, err := tag.Sum(file)
			if err != nil {
				log.Printf("tag.Sum: %v\n", err)
				continue
			}

			pictureHash := ""
			if m.Picture() == nil {
				var ok bool
				dirname := filepath.Dir(ev.Name)
				pictureHash, ok = albumPictures[dirname]
				if !ok {
					picture, err := getAlbumArtFromDir(dirname)
					if err != nil {
						logger.Printf("Failed to get an album art in %v: %v", dirname, err)
					}
					if picture != nil {
						sum := sha256.Sum256(picture.Data)
						pictureHash = base64.StdEncoding.EncodeToString(sum[:])
						albumPictures[dirname] = pictureHash
						err = uploadPicture(ctx, bucket, pictureHash, m.Picture())
					}
				}
			}
			if pictureHash == "" && m.Picture() != nil {
				sum := sha256.Sum256(m.Picture().Data)
				pictureHash = base64.StdEncoding.EncodeToString(sum[:])
				_, ok := albumPictures[pictureHash]
				if !ok {
					albumPictures[pictureHash] = pictureHash
					err = uploadPicture(ctx, bucket, pictureHash, m.Picture())
				}
			}

			key := datastore.IncompleteKey(benten.PieceKind, nil)
			value := benten.NewMetadata(m, pictureHash, hash, ev.Name)
			_, err = datastoreClient.Put(ctx, key, &value)
			if err != nil {
				log.Printf("put: %v\n", err)
				continue
			}
			log.Println("OK!")

		case fsnotify.Write:
		case fsnotify.Remove:
		case fsnotify.Rename:
		case fsnotify.Chmod:

		}

	}
}

func walk(path string, ch chan fsnotify.Event) {
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			logger.Printf("Found: %v\n", path)
			ch <- fsnotify.Event{Op: fsnotify.Create, Name: path}
		}

		return nil
	})
	if err != nil {
		logger.Printf("Error during filepath.Wark: %v\n", err)
	}
}

func get(name string) {
	deadline := 5 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
		return
	}
	bucket := client.Bucket(bucketName)

	object := bucket.Object(name)

	reader, err := object.NewReader(ctx)

	if err != nil {
		log.Fatal(err)
		return
	}

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
		fmt.Print(bs[0:read])
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

	ch := make(chan fsnotify.Event)
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
				ch <- event
			case err, _ = <-watcher.Errors:
				logger.Printf("%v\n", err)
			}
		}
	}()
	sync(ch)
}
