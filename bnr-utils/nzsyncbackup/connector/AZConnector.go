package Connector

import (
    "fmt"
    "strings"
    "strconv"
    "net/url"
    "time"
    "os"
    "path/filepath"
    "path"
    "context"
    "log"
    "github.com/Azure/azure-storage-blob-go/azblob"
)

type iaz interface {
    getServiceURL() (azblob.ServiceURL, error)
    getContainerURL() (azblob.ContainerURL, error)
    getBlockBlobURL(blobname string) (azblob.BlockBlobURL, error) 
    getBlobURL(blobname string) (azblob.BlobURL, error)
    uploadFile(absfilepath string, relfilepath string, uniqueid string) (error)
    downloadFile(outfilepath string, blobname string, streams uint, blockSize int64) error

}

type AZConnector struct {
    azaccount   string
    azkey       string
    azcontainer string
    streams     uint
    blocksize   int64
}

type job struct {
    uniqueid    string
    bkpdir      string
}

type uploadJob struct {
    job
    absfilepath string
}

type jobResult struct {
    *job
    err         error
}

type downloadJob struct {
    conn        AZConnector
    blobname    string
    outfilepath string
}

type downloadJobResult struct {
    blobname    string
    err         error
}

func (c *AZConnector) ParseConnectorArgs(args string) {
    arguments := strings.Split(args, ";")
    for _, arg := range arguments {
        kv := strings.Split(arg, ":")
        switch kv[0] {
        case "STORAGE_ACCOUNT":
            c.azaccount = kv[1]
        case "KEY":
            c.azkey = kv[1]
        case "CONTAINER":
            c.azcontainer = kv[1]
        case "STREAMS":
            u32, err := strconv.ParseUint(kv[1], 10, 32)
            if (err == nil ) {
                c.streams = uint(u32)
            }
        case "BLOCKSIZE":
            u64, err := strconv.ParseInt(kv[1], 10, 64)
            if (err == nil ) {
                c.blocksize = int64(u64)
            }
        }
    }
}

func (j *uploadJob) uploadAZ(conn *AZConnector) error {
    relfilepath, err := filepath.Rel(j.job.bkpdir, j.absfilepath)
    if err != nil {
        return fmt.Errorf("Unable to traverse %s, %s: %v", j.job.bkpdir, j.absfilepath, err)
    }

    log.Println("Uploading file :", j.absfilepath)
    return conn.uploadFile(j.absfilepath, relfilepath, j.job.uniqueid)
}

func (cn *AZConnector) getServiceURL() (azblob.ServiceURL, error) {
    var serviceURL azblob.ServiceURL
    us := fmt.Sprintf("https://%s.blob.core.windows.net/", cn.azaccount)
    u, err := url.Parse(us)
    if err != nil {
        return serviceURL, fmt.Errorf("Unable to parse URL: %s : %v", us, err)
    }

    credential, err := azblob.NewSharedKeyCredential(cn.azaccount, cn.azkey)
    if err != nil {
        return serviceURL, fmt.Errorf("Unable to create shared credentials: %v", err)
    }

    p := azblob.NewPipeline(credential, azblob.PipelineOptions{
            Retry: azblob.RetryOptions{
                    TryTimeout: 5 * time.Minute,
            },
    })

    serviceURL = azblob.NewServiceURL(*u, p)
    return serviceURL, nil
}

func (cn *AZConnector) getContainerURL() (azblob.ContainerURL, error) {
    var containerURL azblob.ContainerURL
    serviceURL, err := cn.getServiceURL()
    if err == nil {
        containerURL = serviceURL.NewContainerURL(cn.azcontainer)
    }
    return containerURL, err
}

func (cn *AZConnector) getBlobURL(blobname string) (azblob.BlobURL, error) {
    var blobURL azblob.BlobURL
    containerURL, err := cn.getContainerURL()
    if err == nil {
        blobURL = containerURL.NewBlobURL(blobname)
    }
    return blobURL, err
}

func (cn *AZConnector) getBlockBlobURL(blobname string) (azblob.BlockBlobURL, error) {
    var blockBlobURL azblob.BlockBlobURL
    containerURL, err := cn.getContainerURL()
    if err == nil {
        blockBlobURL = containerURL.NewBlockBlobURL(blobname)
    }
    return blockBlobURL, err
}

func (cn *AZConnector) uploadFile(absfilepath string, relfilepath string, uniqueid string) (error){
    // Upload the file to a block blob
    blockBlobURL, err := cn.getBlockBlobURL(uniqueid+"/"+relfilepath)
    if err != nil {
        return err
    }

    file, err := os.Open(absfilepath)
    if err != nil {
        return fmt.Errorf("Error in opening backup file : %v", err)
    }

    _, err = azblob.UploadFileToBlockBlob(context.Background(), file, blockBlobURL,
                        azblob.UploadToBlockBlobOptions{
                        BlockSize:   int64(cn.blocksize * 1024 * 1024),
                        Parallelism: uint16(cn.streams),
            })

    return err
}


func (cn *AZConnector) Upload( otherargs *OtherArgs, backupinfo *BackupInfo ) (error){
    var err error
    log.Println("Uploading Using AZ Connector")
    dirlist := strings.Split(backupinfo.Dir," ")
    for _, bkpdir := range dirlist {
        backupdir := filepath.Join(bkpdir, "Netezza", backupinfo.npshost, backupinfo.dbname, backupinfo.backupset, backupinfo.increment)
        _, err = os.Stat(backupdir)
        if err != nil {
            return fmt.Errorf("Error in Creating backupdir : %v", err)
        }
        work := make(chan *uploadJob, otherargs.paralleljobs)
        result := make(chan *jobResult, otherargs.paralleljobs)
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
                    err := j.uploadAZ(cn)
                    jr := jobResult{ job:&j.job, err:err }
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
                        log.Fatalf("%s: %v", r.job, r.err)
                    }
                    filesuploaded++ // this is fine, since this is single threaded increment
                }
            }
        }()

        err = filepath.Walk(backupdir,
            func(absfilepath string, info os.FileInfo, err error) error {
                if info.IsDir() {
                    return nil
                }
                j := uploadJob{ job: job{otherargs.uniqueid, bkpdir}, absfilepath: absfilepath }
                work <- &j  // this will hang until at least one of the prior uploads finish if other.paralleljobs
                            // are already running
                return err
            })
        close(work)
        <- done
        log.Println("Upload successful for Backup Dir  :", bkpdir)
        log.Println("Total files uploaded:", filesuploaded)
    }
    return err
}

func (j *downloadJob) downloadAZ(conn *AZConnector) error {

    log.Println("Downloading file :", j.blobname)
    return conn.downloadFile(j.outfilepath, j.blobname, conn.streams, conn.blocksize)
}

func (cn *AZConnector) downloadFile(outfilepath string, blobname string, streams uint, blockSize int64) error {

    filehandle, err := os.Create(outfilepath)
    if err != nil {
        return fmt.Errorf("Error in creating file inside backup dir: %v",err)
    }

    defer filehandle.Close()

    blobURL, err := cn.getBlobURL(blobname)
    if err != nil {
        return err
    }

    // Perform download
    err = azblob.DownloadBlobToFile(context.Background(), blobURL, 0, 0, filehandle,
                     azblob.DownloadFromBlobOptions{
                     BlockSize:   int64(blockSize * 1024 * 1024),
                     RetryReaderOptionsPerBlock: azblob.RetryReaderOptions{MaxRetryRequests: 20},
                     Parallelism: uint16(streams),
         })
    if err != nil {
        return fmt.Errorf("Error in downloading an Azure blob to a file: %v",err)
    }
    return err
}


func (cn *AZConnector) Download(otherargs *OtherArgs, backupinfo *BackupInfo) (error){
    var err error
    log.Println("Downloading Using AZ Connector")
    outdir := backupinfo.Dir
    arrayLoc:= []string{}
    arrayContents:= []string{}
    work := make(chan *downloadJob, otherargs.paralleljobs)
    result := make(chan *downloadJobResult, otherargs.paralleljobs)
    done := make(chan bool)

    blobpath := filepath.Join(otherargs.uniqueid, "Netezza",backupinfo.npshost, backupinfo.dbname, backupinfo.backupset, backupinfo.increment)
    
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
                err := j.downloadAZ(cn)
                jr := downloadJobResult{ blobname:j.blobname, err:err }
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
                    log.Fatalf("%s: %v", r.blobname, r.err)
                }
                filesdownloaded++ // this is fine, since this is single threaded increment
            }
        }
    }()


    blobfound := 0

    for marker := (azblob.Marker{}); marker.NotDone(); {
        containerURL, err := cn.getContainerURL()
        if err != nil {
            return err
        }

        // Get a result segment starting with the blob indicated by the current Marker.
        listBlob, err := containerURL.ListBlobsFlatSegment(context.Background(), marker, azblob.ListBlobsSegmentOptions{})
        if err != nil {
            return fmt.Errorf("Error in listing segment of blobs: %v",err)
        }

        // ListBlobs returns the start of the next segment; you MUST use this to get
        // the next segment (after processing the current result segment).
        marker = listBlob.NextMarker
        // Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
        for _, blobInfo := range listBlob.Segment.BlobItems {
            if strings.HasPrefix(blobInfo.Name, blobpath) {

                // Set up file to download the blob to
                dir, filename := filepath.Split(blobInfo.Name)

                relfilepath, err := filepath.Rel(otherargs.uniqueid,dir)
                if err != nil {
                    return fmt.Errorf("Error in fetching download relative path: %v",err)
                }

                dumpdir := filepath.Join(outdir, relfilepath)
                err = os.MkdirAll(dumpdir, 0777)
                if err != nil {
                    return fmt.Errorf("Error in creating backup directory structure: %v",err)
                }

                outfilepath := path.Join(dumpdir, filename)
                if (strings.HasSuffix(outfilepath,"locations.txt")){
                    arrayLoc = append(arrayLoc,outfilepath)
                }
                if (strings.HasSuffix(outfilepath,"contents.txt")){
                    arrayContents = append(arrayContents,outfilepath)
                }

                j := downloadJob{ conn:*cn, outfilepath:outfilepath, blobname: blobInfo.Name }
                work <- &j

                blobfound--
            }
        }

        if blobfound > 0 {
            log.Println("No matching blob found. Please check if DB name, hostname, uniqueid or containername is correct")
            return fmt.Errorf("No matching blob found.")
        }
        if blobfound == 0 {
            blobfound++
        }
    }
    close(work)
    <- done
    log.Println("File Downloaded to dir :", outdir)
    log.Println("Total files downloaded:", filesdownloaded)
    if (*otherargs.cloudBackup) {
        updateLocation(arrayLoc,outdir)
        updateContents(arrayContents)
    }
    return err
}
