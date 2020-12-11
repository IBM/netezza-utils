package main

import (
    "flag"
    "fmt"
    "net/url"
    "time"
    "os"
    "context"
    "path/filepath"
    "path"
    "strings"
    "log"
    "github.com/Azure/azure-storage-blob-go/azblob"
)

type Conn struct {
    azaccount   string
    azkey       string
    azcontainer string
    streams     uint
}

type BackupInfo struct {
    dbname      string
    dirs        string
    npshost     string
    backupsetID string
}

type OtherArgs struct {
    uniqueid    string
    logfiledir  string
    upload      *bool
    download    *bool
    paralleljobs int
}

type job struct {
    conn        Conn
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

func (j *uploadJob) upload() error {
    relfilepath, err := filepath.Rel(j.job.bkpdir, j.absfilepath)
    if err != nil {
        return fmt.Errorf("Unable to traverse %s, %s: %v", j.job.bkpdir, j.absfilepath, err)
    }

    log.Println("Uploading file :", j.absfilepath)
    return j.job.conn.uploadFile(j.absfilepath, relfilepath, j.job.uniqueid, j.job.conn.streams)
}

type downloadJob struct {
    conn        Conn
    blobname    string
    outfilepath string
}

type downloadJobResult struct {
    blobname    string
    err         error
}

func (j *downloadJob) download() error {

    log.Println("Downloading file :", j.blobname)
    return j.conn.downloadFile(j.outfilepath, j.blobname, j.conn.streams)
}

func parseArgs(conn *Conn, backupinfo *BackupInfo, othargs *OtherArgs) {
    flag.StringVar(&backupinfo.dbname,"db", "", "Database name")
    flag.StringVar(&backupinfo.dirs,"dir", "", "Full path to the directory in which the backup already exists or should be downloaded")
    flag.StringVar(&backupinfo.npshost,"npshost", "", "Name of the NPS host as it appears in the backups")
    flag.StringVar(&backupinfo.backupsetID,"backupset", "", "Name of the backupset to be uploaded/downloaded")

    flag.StringVar(&conn.azaccount,"storage-account", "", "Azure blob storage account")
    flag.StringVar(&conn.azkey,"key", "", "Azure blob storage access key")
    flag.StringVar(&conn.azcontainer,"container", "", "Azure blob storage container")
    flag.UintVar(&conn.streams,"streams",16,"Number of blocks to upload/download in parallel")

    flag.StringVar(&othargs.uniqueid,"uniqueid", "", "Azure blob storage container")
    flag.StringVar(&othargs.logfiledir,"logfiledir", "/tmp", "Logfile directory for this utility. Default is /tmp dir")
    othargs.upload = flag.Bool("upload", false, "Upload to cloud")
    othargs.download = flag.Bool("download", false, "Download from cloud")
    flag.IntVar(&othargs.paralleljobs,"paralleljobs",6,"Number of parallel files to upload/download")
}


func handleErrors(err error) {
    if err != nil {
        log.Fatalln(err)
    }
}

func (cn *Conn) getServiceURL() (azblob.ServiceURL, error) {
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

func (cn *Conn) getContainerURL() (azblob.ContainerURL, error) {
    var containerURL azblob.ContainerURL
    serviceURL, err := cn.getServiceURL()
    if err == nil {
        containerURL = serviceURL.NewContainerURL(cn.azcontainer)
    }
    return containerURL, err
}

func (cn *Conn) getBlockBlobURL(blobname string) (azblob.BlockBlobURL, error) {
    var blockBlobURL azblob.BlockBlobURL
    containerURL, err := cn.getContainerURL()
    if err == nil {
        blockBlobURL = containerURL.NewBlockBlobURL(blobname)
    }
    return blockBlobURL, err
}

func (cn *Conn) getBlobURL(blobname string) (azblob.BlobURL, error) {
    var blobURL azblob.BlobURL
    containerURL, err := cn.getContainerURL()
    if err == nil {
        blobURL = containerURL.NewBlobURL(blobname)
    }
    return blobURL, err
}

func (cn *Conn) uploadFile(absfilepath string, relfilepath string, uniqueid string, streams uint) (error){
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
                        Parallelism: uint16(streams),
            })

    return err
}

func (cn *Conn) downloadFile(outfilepath string, blobname string, streams uint) error {

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
                     Parallelism: uint16(streams),
         })
    if err != nil {
        return fmt.Errorf("Error in downloading an Azure blob to a file: %v",err)
    }
    return err
}

func (cn *Conn) downloadBkp(outdir string, uniqueid string, blobpath string, streams uint, paralleljobs int) (error){
    var err error
    work := make(chan *downloadJob, paralleljobs)
    result := make(chan *downloadJobResult, paralleljobs)
    done := make(chan bool)

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
                err := j.download()
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

                relfilepath, err := filepath.Rel(uniqueid,dir)
                if err != nil {
                    return fmt.Errorf("Error in fetching download relative path: %v",err)
                }

                dumpdir := filepath.Join(outdir, relfilepath)
                err = os.MkdirAll(dumpdir, 0777)
                if err != nil {
                    return fmt.Errorf("Error in creating backup directory structure: %v",err)
                }

                outfilepath := path.Join(dumpdir, filename)
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
    log.Println("Total files downloaded:", filesdownloaded)
    return err
}

func main() {
    var conn Conn
    var backupinfo BackupInfo
    var othargs OtherArgs

    // parse input args
    parseArgs(&conn, &backupinfo, &othargs)
    flag.Parse()

    if flag.NFlag() == 0 {
        fmt.Println("No arguments passed to nz_azConnector. Below is the list of valid args:")
        flag.PrintDefaults()
    }

    // log file configuration setup
    logfilename := fmt.Sprintf("nz_azConnector_%d_%s.log", os.Getppid(), time.Now().Format("2006-01-02"))
    logfilepath := path.Join(othargs.logfiledir, logfilename)
    filehandle, err := os.OpenFile(logfilepath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
    if err != nil {
        fmt.Errorf("Error in opening logfile: %v",err)
    }
    log.SetOutput(filehandle)
    prefixStr := fmt.Sprintf("%s  ", time.Now().UTC().Format("2006-01-02 15:04:05 EST")) + fmt.Sprintf("%-7s", "[INFO]")
    log.SetFlags(0)
    log.SetPrefix(prefixStr)

    dirlist := strings.Split(backupinfo.dirs," ")
    log.Println("Azure account name :", conn.azaccount)
    log.Println("Azure container :", conn.azcontainer)
    log.Println("Number of blocks to upload/download in parallel :", conn.streams)
    log.Println("Backup/Restore directory :",dirlist)
    log.Println("DB name :", backupinfo.dbname)
    log.Println("Nps hostname :", backupinfo.npshost)
    log.Println("BackupsetID :", backupinfo.backupsetID)
    log.Println("UniqueID :", othargs.uniqueid)
    log.Println("Number of files to upload/download in parallel :", othargs.paralleljobs)

    for _, bkpdir := range dirlist {
        if (*othargs.upload) {

            // now do the upload
            log.Println("Uploading backup data to azure cloud from backup dir", bkpdir)
            backupdir := filepath.Join(bkpdir, "Netezza", backupinfo.npshost, backupinfo.dbname, backupinfo.backupsetID)
            _, err = os.Stat(backupdir)
            handleErrors(err)

            work := make(chan *uploadJob, othargs.paralleljobs)
            result := make(chan *jobResult, othargs.paralleljobs)
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
                        err := j.upload()
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
                    j := uploadJob{ job: job{conn, othargs.uniqueid, bkpdir}, absfilepath: absfilepath }
                    work <- &j  // this will hang until at least one of the prior uploads finish if other.paralleljobs
                                // are already running
                    return err
                })
            close(work)
            <- done
            handleErrors(err)
            log.Println("Upload successful. Total files uploaded:", filesuploaded)
        }

        if (*othargs.download) {
            log.Println("Downloading backup data from azure cloud to restore dir", bkpdir)
            blobpath := filepath.Join(othargs.uniqueid, "Netezza",backupinfo.npshost, backupinfo.dbname, backupinfo.backupsetID)
            err = conn.downloadBkp(bkpdir, othargs.uniqueid, blobpath, conn.streams, othargs.paralleljobs)
            handleErrors(err)
            log.Println("Download successful")
        }
    }
}
