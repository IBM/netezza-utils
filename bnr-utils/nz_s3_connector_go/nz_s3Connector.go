package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Conn struct {
	accessKeyId     string
	bucketUrl       string
	defaultRegion   string
	secretAccessKey string
	endPoint        string
	streams         int64
	blockSize       int64
}

type BackupInfo struct {
	dbname      string
	dirs        string
	npshost     string
	backupsetID string
}

type OtherArgs struct {
	download     *bool
	upload       *bool
	parallelJobs int64
	logFileDir   string
	uniqueId     string
}

func parseArgs(s3Conn *S3Conn, backupinfo *BackupInfo, otherArgs *OtherArgs) {
	flag.StringVar(&backupinfo.dbname, "db", "", "Database name")
	flag.StringVar(&backupinfo.dirs, "dir", "", "Full path to the directory in which the backup already exists or should be downloaded. Enclose in double quotes if there are multiple directories.")
	flag.StringVar(&backupinfo.npshost, "npshost", "", "Name of the NPS host as it appears in the backups")
	flag.StringVar(&backupinfo.backupsetID, "backupset", "", "Name of the backupset to be uploaded/downloaded.")
	flag.StringVar(&otherArgs.logFileDir, "logfiledir", "", "Logfile directory for this utility")

	flag.StringVar(&s3Conn.accessKeyId, "access-key", "", "Access Key Id to access AWS s3/IBM cloud")
	flag.StringVar(&s3Conn.bucketUrl, "bucket-url", "", "Bucket url to access AWS s3/IBM cloud")
	flag.StringVar(&s3Conn.defaultRegion, "region", "", "Default region of your bucket in AWS s3/IBM cloud")
	flag.StringVar(&s3Conn.secretAccessKey, "secret-key", "", "Secret Access Key to access access AWS s3/IBM cloud")
	flag.StringVar(&s3Conn.endPoint, "endpoint", "", "URL of the entry point for an AWS s3/IBM cloud. Mandatory for IBM cloud service.")
	flag.Int64Var(&s3Conn.streams, "streams", 16, "Number of blocks to upload/download in parallel default 16")
	flag.Int64Var(&s3Conn.blockSize, "blocksize", 100, "Block size in MB to upload/download file")

	otherArgs.download = flag.Bool("download", false, "Download from cloud")
	otherArgs.upload = flag.Bool("upload", false, "Upload from cloud")
	flag.Int64Var(&otherArgs.parallelJobs, "paralleljobs", 6, "Parallel jobs for upload/download")
	flag.StringVar(&otherArgs.uniqueId, "unique-id", "", "Unique ID associated with the file transfer")
}

func main() {
	var conn S3Conn
	var backupinfo BackupInfo
	var otherArgs OtherArgs

	// parse input args
	parseArgs(&conn, &backupinfo, &otherArgs)
	flag.Parse()
	prefixStr := fmt.Sprintf("%s  ", time.Now().UTC().Format("2006-01-02 15:04:05")) + fmt.Sprintf("%-7s", "[INFO]")
	if otherArgs.logFileDir != "" {
		log.Printf("logfile dir: %s", otherArgs.logFileDir)
		logfilename := fmt.Sprintf("nz_s3Connector_%d_%s.log", os.Getppid(), time.Now().Format("2006-01-02"))
		logfilepath := path.Join(otherArgs.logFileDir, logfilename)
		f, err := os.OpenFile(logfilepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("error opening log file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}
	log.SetFlags(0)
	log.SetPrefix(prefixStr)
	if flag.NFlag() == 0 {
		log.Println("No arguments passed to nz_s3Connector. Below is the list of valid args: ")
		flag.PrintDefaults()
		os.Exit(1)
	}

	log.Println("Aws S3 bucket:", conn.bucketUrl)
	log.Println("Aws region:", conn.defaultRegion)
	log.Println("Backup/Restore directory:", backupinfo.dirs)
	log.Println("DB name :", backupinfo.dbname)
	log.Println("Nps hostname :", backupinfo.npshost)
	if backupinfo.backupsetID != "" {
		log.Println("BackupsetID :", backupinfo.backupsetID)
	} else {
		log.Println("BackupsetID : ALL")
	}
	log.Println("Number of files to upload/download in parallel :", otherArgs.parallelJobs)

	cfg := conn.createS3Config()
	if *otherArgs.download {
		conn.Download(cfg, backupinfo, otherArgs)
		log.Println("Downloading complete.")
	}
	if *otherArgs.upload {
		conn.Upload(cfg, backupinfo, otherArgs)
		log.Println("Uploading complete.")
	}
}

func (s3Conn *S3Conn) Upload(cfg aws.Config, bkp BackupInfo, otherArgs OtherArgs) {
	dirlist := strings.Split(bkp.dirs, " ")
	for _, dir := range dirlist {
		backupdir := filepath.Join(dir, "Netezza", bkp.npshost, bkp.dbname, bkp.backupsetID)
		_, err := os.Stat(backupdir)
		if err != nil {
			log.Fatalf("Cannot access directory %s: %v. Please check if DB name, hostname are correct.", backupdir, err)
		}
		log.Printf("Uploading data to s3 bucket %s with unique-id %s from dir %s", s3Conn.bucketUrl, otherArgs.uniqueId, backupdir)

		filesuploaded := 0
		var wg sync.WaitGroup
		var mu sync.Mutex

		// buffered channel to limit concurrency
		sem := make(chan struct{}, otherArgs.parallelJobs)
		err = filepath.Walk(backupdir, func(path string, info fs.FileInfo, err error) error {
			if info.IsDir() {
				return nil
			}

			relfilepath, patherr := filepath.Rel(dir, path)
			if patherr != nil {
				return patherr
			}

			wg.Add(1)
			sem <- struct{}{}

			go func() {
				err := s3Conn.uploadFileToS3(path, cfg, otherArgs.uniqueId, relfilepath)
				if err != nil {
					log.Println("Error while uploading file. Ensure aws s3 access-key-id, secret-access-key, bucket_url are correct.")
					log.Fatalf("Failed to upload file. Err: %v", err)
				}
				log.Printf("File %s uploaded successfully", path)
				mu.Lock()
				filesuploaded++
				mu.Unlock()
				wg.Done()
				<-sem
			}()
			return err
		})
		if err != nil {
			log.Fatalf("Encountered error while traversing the directory. Err: %v", err)
		}
		wg.Wait()
		log.Printf("Total files uploaded: %d", filesuploaded)
	}
}

func (s3Conn *S3Conn) getUploader(cfg aws.Config) *manager.Uploader {
	return manager.NewUploader(s3.NewFromConfig(cfg), func(u *manager.Uploader) {
		u.PartSize = s3Conn.blockSize * 1024 * 1024
		u.Concurrency = int(s3Conn.streams)
	})
}

func (s3Conn *S3Conn) uploadFileToS3(absFilePath string, cfg aws.Config, uniqueId string, relFilePath string) error {
	uploader := s3Conn.getUploader(cfg)
	f, err := os.Open(absFilePath)
	if err != nil {
		log.Printf("Unable to open file %s. Err: %v", absFilePath, err)
		return err
	}
	defer f.Close()

	_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s3Conn.bucketUrl),
		Body:   f,
		Key:    aws.String(filepath.Join(uniqueId, relFilePath)),
	})
	if err != nil {
		log.Fatalf("Failed to upload file: %s. Err: %v", absFilePath, err)
	}
	return nil
}

func (s3Conn *S3Conn) Download(cfg aws.Config, bkp BackupInfo, otherArgs OtherArgs) {
	bkpath := filepath.Join(otherArgs.uniqueId, "Netezza", bkp.npshost, bkp.dbname, bkp.backupsetID)
	log.Printf("Backup dir path: %s", bkpath)
	dirlist := strings.Split(bkp.dirs, " ")

	for _, dir := range dirlist {
		log.Printf("Downloading data to dir %s", dir)
		client := s3.NewFromConfig(cfg)
		filesdownloaded := 0
		var wg sync.WaitGroup
		var mu sync.Mutex

		// buffered channel to limit concurrency
		sem := make(chan struct{}, otherArgs.parallelJobs)

		paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
			Bucket: aws.String(s3Conn.bucketUrl),
			Prefix: aws.String(otherArgs.uniqueId),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(context.TODO())
			if err != nil {
				log.Fatalf("Error while listing objects: %v", err)
			}

			for _, obj := range page.Contents {
				key := *obj.Key
				if strings.HasPrefix(key, bkpath) {
					splitdir, filename := filepath.Split(key)
					relfilepath, err := filepath.Rel(otherArgs.uniqueId, splitdir)
					if err != nil {
						log.Fatalf("Error in fetching download relative path: %v", err)
					}

					dumpdir := filepath.Join(dir, relfilepath)
					err = os.MkdirAll(dumpdir, 0777)
					if err != nil {
						log.Fatalf("Error in creating backup directory: %v", err)
					}

					outfilepath := path.Join(dumpdir, filename)
					wg.Add(1)
					sem <- struct{}{}

					go func() {
						err := s3Conn.downloadFileFromS3(outfilepath, cfg, key)
						if err != nil {
							log.Println("Error while downloading file. Ensure aws s3 access-key-id, secret-access-key, bucket_url are correct.")
							log.Fatalf("Failed to download file. Err: %v", err)
						}
						log.Printf("File %s downloaded successfully", key)
						mu.Lock()
						filesdownloaded++
						mu.Unlock()
						wg.Done()
						<-sem
					}()
				}
			}
		}
		wg.Wait()
		log.Printf("Total files downloaded: %d", filesdownloaded)
	}
}

func (s3Conn *S3Conn) getDownloader(cfg aws.Config) *manager.Downloader {
	return manager.NewDownloader(s3.NewFromConfig(cfg), func(d *manager.Downloader) {
		d.PartSize = s3Conn.blockSize * 1024 * 1024
		d.Concurrency = int(s3Conn.streams)
	})
}

func (s3Conn *S3Conn) downloadFileFromS3(absFilePath string, cfg aws.Config, relFilePath string) error {
	downloader := s3Conn.getDownloader(cfg)
	f, err := os.Create(absFilePath)
	if err != nil {
		log.Printf("Unable to create file %s. Err: %v", absFilePath, err)
		return err
	}
	defer f.Close()

	bytes, err := downloader.Download(context.TODO(), f, &s3.GetObjectInput{
		Bucket: aws.String(s3Conn.bucketUrl),
		Key:    aws.String(relFilePath),
	})
	if err != nil || bytes < 0 {
		log.Fatalf("Failed to download file: %s. Err: %v", relFilePath, err)
		return err
	}
	return nil
}

func (s3Conn *S3Conn) createS3Config() aws.Config {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(s3Conn.defaultRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			s3Conn.accessKeyId,
			s3Conn.secretAccessKey,
			"",
		)),
	)
	if err != nil {
		log.Fatalf("Failed to create AWS config: %v", err)
	}
	cfg.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	cfg.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired

	if s3Conn.endPoint != "" {
		cfg.BaseEndpoint = aws.String(s3Conn.endPoint)
	}

	return cfg
}
