package Connector

import (
    "fmt"
    "strings"
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
    Azaccount   string
    Azkey       string
    Azcontainer string
    Streams     uint
    Blocksize   int64
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
    us := fmt.Sprintf("https://%s.blob.core.windows.net/", cn.Azaccount)
    u, err := url.Parse(us)
    if err != nil {
        return serviceURL, fmt.Errorf("Unable to parse URL: %s : %v", us, err)
    }

    credential, err := azblob.NewSharedKeyCredential(cn.Azaccount, cn.Azkey)
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
        containerURL = serviceURL.NewContainerURL(cn.Azcontainer)
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
                        BlockSize:   int64(cn.Blocksize * 1024 * 1024),
                        Parallelism: uint16(cn.Streams),
            })

    return err
}


func (cn *AZConnector) Upload( otherargs *OtherArgs, backupinfo *BackupInfo ) (error){
    var err error
    dirlist := strings.Split(backupinfo.Dir," ")
    for _, bkpdir := range dirlist {
        log.Println("Uploading backup data to azure cloud from backup dir", bkpdir)
        backupdir := filepath.Join(bkpdir, "Netezza", backupinfo.npshost, backupinfo.dbname, backupinfo.backupset, backupinfo.increment)
        _, err = os.Stat(backupdir)
        if err != nil {
            return fmt.Errorf("Cannot access directory '%s': %v. Please check if DB name, hostname are correct.", backupdir, err)
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
                        log.Println("Error while uploading file. Ensure azure storage account name, azure key and container name are correct. If error persists contact IBM support team.", *r.job)
                        log.Fatalf("Azure storage account:%s accessing container:%s failed with error: %v", cn.Azaccount, cn.Azcontainer, r.err)
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
        if (err != nil) {
            return(fmt.Errorf("Error reading directory: %s: %v. Please check if DB name, hostname are correct.", backupdir, err))
        }
        log.Println("Upload successful for Backup Dir  :", bkpdir)
        log.Println("Total files uploaded:", filesuploaded)
    }
    return err
}

func (j *downloadJob) downloadAZ(conn *AZConnector) error {

    log.Println("Downloading file :", j.blobname)
    return conn.downloadFile(j.outfilepath, j.blobname, conn.Streams, conn.Blocksize)
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
    log.Println("Downloading backup data from azure cloud to backup dir", backupinfo.Dir)
    outdir := backupinfo.Dir
    locations:= []string{}
    contents:= []string{}
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
            return fmt.Errorf("Unable to list segment of blobs with storage account:%s and container:%s. Ensure azure storage account and container are correct.\n Error details: %v",cn.Azaccount, cn.Azcontainer, err)
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
                
                switch filename {
                case "locations.txt":
                    locations = append(locations, path.Join(dumpdir, filename))
                case "contents.txt":
                    contents = append(contents, path.Join(dumpdir, filename))
                }

                outfilepath := path.Join(dumpdir, filename)

                j := downloadJob{ conn:*cn, outfilepath:outfilepath, blobname: blobInfo.Name }
                work <- &j

                blobfound--
            }
        }

        if blobfound == 0 {
            return fmt.Errorf("No matching blob found. Please check if DB name, hostname, uniqueid or containername are correct. If error persists contact IBM support team. Azaccount:%s AzContainer:%s Blobpath:%s, Uniqueid:%s", cn.Azaccount, cn.Azcontainer, blobpath, otherargs.uniqueid)
        }
    }
    close(work)
    <- done
    log.Println("File Downloaded to dir :", outdir)
    log.Println("Total files downloaded:", filesdownloaded)
    updateLocation(locations,outdir)
    updateContents(contents)
    return err
}
