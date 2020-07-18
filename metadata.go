package benten

import (
	"cloud.google.com/go/datastore"
	"github.com/dhowden/tag"
)

// Metadata epresents a metadata of an audio file. This is equivalent to tag.Metadata except for the following members:
//  - Picture
//  - Hash
//  - Path
type Metadata struct {
	// Format is the metadata Format used to encode the data.
	Format string
	// FileType is the file type of the audio file.
	FileType string
	// Title is the title of the track.
	Title string
	// Album is the album name of the track.
	Album string
	// Artist is the artist name of the track.
	Artist string
	// AlbumArtist is the album artist name of the track.
	AlbumArtist string
	// Composer is the name of the composer of the track.
	Composer string
	// Genre is the genre of the track.
	Genre string
	// Year is the year of the track.
	Year int

	// Track is the track number of this piece in the album, or zero values if unavailable.
	Track int
	// TotalTracks is the number of tracks in the album, or zero values if unavailable.
	TotalTracks int
	// Disc returns the disc number in the album, or zero values if unavailable.
	Disc int
	// TotalDiscs is the number discs in the album, or zero values if unavailable.
	TotalDisks int

	// Comment is the comment, or an empty string if unavailable.
	Comment string

	// The key of the item stored in the datastore which represents the picture of the file, or the empty string if unavailable.
	Picture string
	// The metadata-invariant checksum: see
	// https://github.com/dhowden/tag#audio-data-checksum-sha1.
	Hash string
	// The relative Path of the file stored in the client storage.
	Path string
}

// NewMetadata creates a Metadata from a tag.Metadata and
func NewMetadata(src tag.Metadata, picture string, hash string, path string) Metadata {
	var dest Metadata

	dest.Format = string(src.Format())
	dest.FileType = string(src.FileType())
	dest.Title = src.Title()
	dest.Album = src.Album()
	dest.Artist = src.Artist()
	dest.AlbumArtist = src.AlbumArtist()
	dest.Composer = src.Composer()
	dest.Genre = src.Genre()
	dest.Year = src.Year()

	dest.Track, dest.TotalTracks = src.Track()
	dest.Disc, dest.TotalDisks = src.Disc()

	dest.Comment = src.Comment()

	dest.Picture = picture
	dest.Hash = hash
	dest.Path = path

	return dest
}

// PieceIndex is an entry of index from text in a Metadata to the key of the Metadata.
type PieceIndex struct {
	Key []byte

	Value *datastore.Key
}
