package Connector

import (
    "fmt"
    "strings"
    "os"
    "path/filepath"
    "path"
    "log"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"
    "github.com/aws/aws-sdk-go/aws/credentials"
)

type is3 interface {
    getUploader()(*s3manager.Uploader, error)
    getDownloader()(*s3manager.Downloader, error)
    getSession() (sess *session.Session)
    uploadFile(absfilepath string, relfilepath string, uniqueid string) (error)
    downloadFile(outfilepath string, key string, conn *s3manager.Downloader) (error) 
}

type S3connector struct {
    Access_key_id string
    Secret_access_key string
    Default_region string
    Bucket_url string
    Endpoint string
    Streams     int
    Blocksize   int64
}

type jobS3 struct {
    uniqueid    string
    bkpdir      string
}

type uploadJobS3 struct {
    jobS3
    absfilepath string
}

type jobResultS3 struct {
    *jobS3
    err         error
}

type downloadJobS3 struct {
    conn        *s3manager.Downloader
    key         string
    outfilepath string
}

type downloadJobResultS3 struct {
    key    string
    err    error
}


func (j *uploadJobS3) uploadS3(conn *S3connector) error {
    relfilepath, err := filepath.Rel(j.jobS3.bkpdir, j.absfilepath)
    if err != nil {
        return fmt.Errorf("Unable to traverse %s, %s: %v", j.jobS3.bkpdir, j.absfilepath, err)
    }

    log.Println("Uploading file :", j.absfilepath)
    return conn.uploadFileS3(j.absfilepath, relfilepath, j.uniqueid)
}

func (c *S3connector) uploadFileS3(absfilepath string, relfilepath string, uniqueid string) (error){
    // Upload the file to a block blob
    uploader  := c.getUploader()

    file, err := os.Open(absfilepath)
    if err != nil {
        return fmt.Errorf("Error in opening backup file : %v", err)
    }

    result,err := uploader.Upload(&s3manager.UploadInput{
            Bucket: &c.Bucket_url,
            Key:    aws.String(filepath.Join(uniqueid, relfilepath)),
            Body:   file,
        })
    
    if (result == nil) {
        return fmt.Errorf("Error in uploading  file : %v", err)
    }
    return err
}

func (j *downloadJobS3) downloadS3(c *S3connector) error {
    log.Println("Downloading file :", j.key)
    return c.downloadFile(j.outfilepath, j.key, j.conn )
}

func (c *S3connector) downloadFile(outfilepath string, key string, conn *s3manager.Downloader) error {
    filehandle, err := os.Create(outfilepath)
    if err != nil {
        return fmt.Errorf("Error in creating file inside backup dir: %v",err)
    }

    defer filehandle.Close()

    numBytes, err := conn.Download(filehandle,
        &s3.GetObjectInput{
            Bucket: aws.String(c.Bucket_url),
            Key:    aws.String(key),
        })

    if((numBytes < 0) && (err != nil)){
        return fmt.Errorf("Error in downloading File: %v",err)
    }

    return err
}

func (c *S3connector) getSession() (sess *session.Session) {
   sess, err := session.NewSession(&aws.Config{
        Region: aws.String(c.Default_region),
        Credentials: credentials.NewStaticCredentials(c.Access_key_id,c.Secret_access_key,""),
        CredentialsChainVerboseErrors: aws.Bool(true) })
    if (err != nil) {
        log.Fatalln("Session failed:", err)
    }

    return sess
} 

func (c *S3connector) getUploader() (*s3manager.Uploader) {
  
    sessn := c.getSession() 
    uploader := s3manager.NewUploader(sessn,func(u *s3manager.Uploader) {
         u.PartSize = c.Blocksize * 1024 * 1024 // 64MB per part
         u.Concurrency = c.Streams
         })
    
    return uploader
}

func (c *S3connector) getDownloader() (*s3manager.Downloader, *session.Session) {
  
    sessn := c.getSession() 
    downloader := s3manager.NewDownloader(sessn,func(d *s3manager.Downloader) {
         d.PartSize = c.Blocksize * 1024 * 1024 // 64MB per part
         d.Concurrency = c.Streams
         })
    
    return downloader,sessn
}

func (c *S3connector) Upload( otherargs *OtherArgs, backupinfo *BackupInfo ) (error){
    var err error
    log.Println("Uploading Using S3 Connector")

    dirlist := strings.Split(backupinfo.Dir," ")
    for _, bkpdir := range dirlist {
        log.Println("Uploading backup data to aws s3 cloud from backup dir", bkpdir)
        backupdir := filepath.Join(bkpdir, "Netezza", backupinfo.npshost, backupinfo.dbname, backupinfo.backupset, backupinfo.increment)
        _, err = os.Stat(backupdir)
        if err != nil {
            return fmt.Errorf("Cannot access directory '%s': %v. Please check if DB name, hostname are correct.", backupdir, err)
        }

        work := make(chan *uploadJobS3, otherargs.paralleljobs)
        result := make(chan *jobResultS3, otherargs.paralleljobs)
        done := make(chan bool)

        go func() {
            for {
                select {
                case j, ok := <- work:
                    if ! ok {
                        // done
                        close(result)
                        return
                    }
                    err := j.uploadS3(c)
                    jr := jobResultS3{jobS3:&j.jobS3, err:err }
                    result <- &jr
                }
            }
        }()

        filesuploaded := 0
        go func() {
            for {
                select {
                    case r, ok := <- result:
                        if ! ok {
                            // work done
                            done <- true
                            return
                    }
                    if r.err != nil {
                        // stopping right here so that we
                        // don't keep on uploading when one has failed
                        log.Println("Error while uploading file. Ensure aws s3 access-key-id, secret-access-key, bucket_url are correct. If error persists contact IBM support team.", *r.jobS3)
                        log.Fatalf("Failed to access AWS bucket: %s with error: %v", c.Bucket_url, r.err)
                    }
                    filesuploaded++ // this is fine, since this is single threaded increment
                }
            }
        }()

        err = filepath.Walk(bkpdir,
            func(absfilepath string, info os.FileInfo, err error) error {
                if info.IsDir() {
                    return nil
                }
                if ( strings.HasPrefix(absfilepath, backupdir) ) {
                    j := uploadJobS3{ jobS3: jobS3{otherargs.uniqueid, bkpdir}, absfilepath: absfilepath }
                    work <- &j  // this will hang until at least one of the prior uploads finish if other.paralleljobs
                                // are already running
                }
                return err
            })

        close(work)
        <- done
        if (err != nil) {
            return(fmt.Errorf("Error reading directory: %s: %v. Please check if DB name, hostname are correct.", backupdir, err))
        }
        log.Println("Upload using S3 connector successful for directory :", bkpdir)
        log.Println("Total files uploaded:", filesuploaded)
    }
    return err
}

func (cn *S3connector) Download(otherargs *OtherArgs, backupinfo *BackupInfo) (error){
    log.Println("Downloading backup data from aws cloud to backup dir", backupinfo.Dir)
    var err error
    outdir := backupinfo.Dir
    locations:= []string{}
    contents:= []string{}
    work := make(chan *downloadJobS3, otherargs.paralleljobs)
    result := make(chan *downloadJobResultS3, otherargs.paralleljobs)
    done := make(chan bool)

    bkpath := filepath.Join(otherargs.uniqueid, "Netezza",backupinfo.npshost, backupinfo.dbname, backupinfo.backupset, backupinfo.increment)
    // start the workers
    go func() {
        for {
            select {
            case j, ok := <- work:
                if ! ok {
                    // done
                    close(result)
                    return
                }
                err := j.downloadS3(cn)
                jr := downloadJobResultS3{ key:j.key, err:err }
                result <- &jr
            }
        }
    }()
    filesdownloaded := 0
    go func() {
        for {
            select {
            case r, ok := <- result:
                if ! ok {
                    // work done
                    done <- true
                    return
                }
                if r.err != nil {
                    // stopping right here so that we
                    // don't keep on uploading when one has failed
                    log.Fatalf("%s: %v", r.key, r.err)
                }
                filesdownloaded++ // this is fine, since this is single threaded increment
            }
        }
    }()

    down,sess := cn.getDownloader()
    client := s3.New(sess)
    params := &s3.ListObjectsInput{Bucket: &cn.Bucket_url, Prefix: &otherargs.uniqueid}
    client.ListObjectsPages(params, func(page *s3.ListObjectsOutput, more bool) (bool) {
        for _, obj := range page.Contents {
            key := *obj.Key
            if strings.HasPrefix(key, bkpath){
                // Create the directories in the path

                dir, filename := filepath.Split(key)
                relfilepath, err := filepath.Rel(otherargs.uniqueid,dir)

                if err != nil {
                    log.Fatalf("Error in fetching download relative path: %v",err)
                }

                dumpdir := filepath.Join(outdir, relfilepath)
                err = os.MkdirAll(dumpdir, 0777)
                if err != nil {
                    log.Fatalf("Error in creating backup directory structure: %v",err)
                }

                switch filename {
                case "locations.txt":
                    locations = append(locations, path.Join(dumpdir, filename))
                case "contents.txt":
                    contents = append(contents, path.Join(dumpdir, filename))
                }

                outfilepath := path.Join(dumpdir, filename)
                j := downloadJobS3{ conn:down, key:key, outfilepath:outfilepath }
                work <- &j
            }
        }
        return true
    })

    close(work)
    <- done
    log.Println("Total files downloaded using S3 Connector:", filesdownloaded)
    updateLocation(locations,outdir)
    updateContents(contents)
    return err
}
