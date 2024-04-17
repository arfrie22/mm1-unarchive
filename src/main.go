package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"errors"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/CorentinB/warc"
	ash0 "github.com/PretendoNetwork/ASH0"
	"github.com/k0kubun/go-ansi"
	"github.com/klauspost/compress/zstd"
	"github.com/schollz/progressbar/v3"
)

// splitAsh0Bundle splits the raw data into separate ASH0 bundles ignoring any data before the first "ASH0" header.
func splitAsh0Bundle(data []byte) [][]byte {
	var splitData [][]byte
	start := -1
	for i := 0; i < len(data); i++ {
		if data[i] == 'A' && data[i+1] == 'S' && data[i+2] == 'H' && data[i+3] == '0' {
			if start != -1 {
				splitData = append(splitData, data[start:i])
			}
			start = i
		}
	}
	splitData = append(splitData, data[start:])

	return splitData
}

// convertLevelData converts the raw level data from the WARC file to a tar.zst file of the format:
//
//	<id>.tar.zst (tar.zst) - Main tar file
//	thumbnail0.tnl (8 byte checksum + JPEG data) - Level preview of main world
//	course_data.cdt (cdt level data) - Main world data
//	course_data_sub.cdt (cdt level data) - Sub world data
//	thumbnail1.tnl (8 byte checksum + JPEG data) - Level thumbnail
func convertLevelData(id string, data []byte) error {
	file, err := os.Create("output/" + id + ".tar.zst")
	if err != nil {
		return err
	}
	defer file.Close()

	zstWriter, err := zstd.NewWriter(file, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return err
	}
	defer zstWriter.Close()

	tarWriter := tar.NewWriter(zstWriter)
	defer tarWriter.Close()

	splitData := splitAsh0Bundle(data)
	fileNames := []string{"thumbnail0.tnl", "course_data.cdt", "course_data_sub.cdt", "thumbnail1.tnl"}

	if len(splitData) != len(fileNames) {
		println(len(splitData), len(fileNames))
		for i, d := range splitData {
			println(i, len(d))
		}
		return errors.New("failed to split data")
	}

	for i, fileName := range fileNames {
		decompressed := ash0.Decompress(splitData[i])
		if decompressed == nil {
			return errors.New("failed to decompress")
		}

		// Write the file to the tar archive compressed with zstd
		err = tarWriter.WriteHeader(&tar.Header{
			Name:    fileName,
			Size:    int64(len(decompressed)),
			Mode:    0644,
			ModTime: time.Now(),
		})

		if err != nil {
			return err
		}

		_, err = tarWriter.Write(decompressed)
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	os.MkdirAll("output/", 0755)
	args := os.Args[1:]
	if len(args) == 0 {
		log.Fatal("No file specified, please provide a .warc.gz or .warc.os.cdx.gz file")
	}

	var archiveFile string
	if strings.HasSuffix(args[0], ".warc.gz") {
		archiveFile = strings.TrimSuffix(args[0], ".warc.gz")
	} else if strings.HasSuffix(args[0], ".warc.os.cdx.gz") {
		archiveFile = strings.TrimSuffix(args[0], ".warc.os.cdx.gz")
	} else {
		log.Fatal("Invalid file type, must be a .warc.gz or .warc.os.cdx.gz file, and both should be in the same directory")
	}

	file, err := os.OpenFile(archiveFile+".warc.os.cdx.gz", os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(gzipReader)

	count := 0
	for scanner.Scan() {
		count++
	}

	// Remove the header line
	count -= 1

	file, err = os.OpenFile(archiveFile+".warc.gz", os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader, err := warc.NewReader(file)
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	name := strings.Split(archiveFile, "/")
	bar := progressbar.NewOptions(count,
		progressbar.OptionSetWriter(ansi.NewAnsiStdout()),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan]["+name[len(name)-1]+"][reset] Processing files"),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionShowElapsedTimeOnFinish(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	for {
		record, err := reader.ReadRecord()
		if err != nil {
			break
		}

		if record.Header.Get("WARC-Type") != "response" {
			continue
		}

		link, err := url.Parse(record.Header.Get("WARC-Target-URI"))
		if err != nil {
			log.Fatal(err)
		}

		pathParts := strings.Split(link.Path, "/")
		fileName := pathParts[len(pathParts)-1]

		data, err := io.ReadAll(record.Content)
		if err != nil {
			log.Fatal(err)
		}

		if err != nil {
			log.Fatal(err)
		}

		err = convertLevelData(fileName, data)
		if err != nil {
			log.Fatal(err)
		}

		bar.Add(1)
	}
}
