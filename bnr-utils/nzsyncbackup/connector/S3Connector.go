package Connector

import (
    "fmt"
    "strings"
    "strconv"
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
    access_key_id string
    secret_access_key string
    default_region string
    bucket_url string
    endpoint string
    streams     int
    blocksize   int64
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
            Bucket: &c.bucket_url,
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
            Bucket: aws.String(c.bucket_url),
            Key:    aws.String(key),
        })

    if((numBytes < 0) && (err != nil)){
        return fmt.Errorf("Error in downloading File: %v",err)
    }

    return err
}

func (c *S3connector) getSession() (sess *session.Session) {
   sess, err := session.NewSession(&aws.Config{
        Region: aws.String(c.default_region),
        Credentials: credentials.NewStaticCredentials(c.access_key_id,c.secret_access_key,""),
        CredentialsChainVerboseErrors: aws.Bool(true) })
    if (err != nil) {
        log.Fatalln("Session failed:", err)
    }

    return sess
} 

func (c *S3connector) getUploader() (*s3manager.Uploader) {
  
    sessn := c.getSession() 
    uploader := s3manager.NewUploader(sessn,func(u *s3manager.Uploader) {
         u.PartSize = c.blocksize * 1024 * 1024 // 64MB per part
         u.Concurrency = c.streams
         })
    
    return uploader
}

func (c *S3connector) getDownloader() (*s3manager.Downloader, *session.Session) {
  
    sessn := c.getSession() 
    downloader := s3manager.NewDownloader(sessn,func(d *s3manager.Downloader) {
         d.PartSize = c.blocksize * 1024 * 1024 // 64MB per part
         d.Concurrency = c.streams
         })
    
    return downloader,sessn
}

func (c *S3connector) ParseConnectorArgs(args string) {
    arguments := strings.Split(args, ";")
    for _, arg := range arguments {
        kv := strings.Split(arg, ":")
        switch kv[0] {
        case "ACCESS_KEY_ID":
            c.access_key_id = kv[1]
        case "SECRET_ACCESS_KEY":
            c.secret_access_key = kv[1]
        case "DEFAULT_REGION":
            c.default_region = kv[1]
        case "BUCKET_URL":
            c.bucket_url = kv[1]
        case "ENDPOINT":
            c.endpoint = kv[1]
        case "STREAMS":
            i, err := strconv.Atoi(kv[1])
            if (err == nil ) {
                c.streams = i
            }
        case "BLOCKSIZE":
            u64, err := strconv.ParseInt(kv[1], 10, 64)
            if (err == nil ) {
                c.blocksize = int64(u64)
            }

        }
    }
}

func (c *S3connector) Upload( otherargs *OtherArgs, backupinfo *BackupInfo ) (error){
    var err error
    log.Println("Uploading Using S3 Connector")

    dirlist := strings.Split(backupinfo.Dir," ")
    for _, bkpdir := range dirlist {
        backupdir := filepath.Join(bkpdir, "Netezza", backupinfo.npshost, backupinfo.dbname, backupinfo.backupset, backupinfo.increment)
        _, err = os.Stat(backupdir)
        if err != nil {
            return fmt.Errorf("Error Directory not present : %v", err)
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
                        log.Fatalf("%s: %v", r.jobS3, r.err)
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
        log.Println("Upload using S3 connector successful for directory :", bkpdir)
        log.Println("Total files uploaded:", filesuploaded)
    }
    return err
}

func (cn *S3connector) Download(otherargs *OtherArgs, backupinfo *BackupInfo) (error){
    log.Println("Downloading Using S3 Connector")
    var err error
    outdir := backupinfo.Dir
    arrayLoc:= []string{}
    arrayContents:= []string{}
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
    params := &s3.ListObjectsInput{Bucket: &cn.bucket_url, Prefix: &otherargs.uniqueid}
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

                file := filepath.Join(outdir, relfilepath)
                err = os.MkdirAll(file, 0777)
                if err != nil {
                    log.Fatalf("Error in creating backup directory structure: %v",err)
                }

                outfilepath := path.Join(file, filename)
                if (strings.HasSuffix(outfilepath,"locations.txt")){
                    arrayLoc = append(arrayLoc,outfilepath)
                }
                if (strings.HasSuffix(outfilepath,"contents.txt")){
                    arrayContents = append(arrayContents,outfilepath)
                }
                j := downloadJobS3{ conn:down, key:key, outfilepath:outfilepath }
                work <- &j
            }
        }
        return true
    })

        close(work)
        <- done
        log.Println("Total files downloaded using S3 Connector:", filesdownloaded)
        if (*otherargs.cloudBackup) {
            updateLocation(arrayLoc,outdir)
            updateContents(arrayContents)
        }
    return err
}
