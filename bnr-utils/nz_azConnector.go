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
    azaccount      string
    azkey          string
    azcontainer    string
	parallelism    uint
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
}

func parseArgs(conn *Conn, backupinfo *BackupInfo, othargs *OtherArgs) {
	flag.StringVar(&backupinfo.dbname,"db", "", "Database name")
	flag.StringVar(&backupinfo.dirs,"dir", "", "Full path to the directory in which the backup already exists or should be downloaded")
	flag.StringVar(&backupinfo.npshost,"npshost", "", "Name of the NPS host as it appears in the backups")
	flag.StringVar(&backupinfo.backupsetID,"backupset", "", "Name of the backupset to be uploaded/downloaded")

	flag.StringVar(&conn.azaccount,"storage-account", "", "Azure blob storage account")
	flag.StringVar(&conn.azkey,"key", "", "Azure blob storage access key")
	flag.StringVar(&conn.azcontainer,"container", "", "Azure blob storage container")
	flag.UintVar(&conn.parallelism,"parallelism",16,"Azure Page size in MB")

	flag.StringVar(&othargs.uniqueid,"uniqueid", "", "Azure blob storage container")
	flag.StringVar(&othargs.logfiledir,"logfiledir", "/tmp", "Logfile directory for this utility. Default is /tmp dir")
	othargs.upload = flag.Bool("upload", false, "Upload to cloud")
	othargs.download = flag.Bool("download", false, "Download from cloud")
}

func handleErrors(customerrmsg string,err error) {
	if err != nil {
		log.Println(customerrmsg)
		log.Fatalln(err)
	}
}

func (cn *Conn) getServiceURL() (azblob.ServiceURL) {
	var serviceURL azblob.ServiceURL
	u, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/", cn.azaccount))
	handleErrors("Error in parsing azure url",err)

	credential, err := azblob.NewSharedKeyCredential(cn.azaccount, cn.azkey)
	handleErrors("Error in creating SharedKeyCredential",err)

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{
			Retry: azblob.RetryOptions{
					TryTimeout: 5 * time.Minute,
			},
	})

	serviceURL = azblob.NewServiceURL(*u, p)
	return serviceURL
}

func (cn *Conn) getContainerURL() (azblob.ContainerURL) {
	var containerURL azblob.ContainerURL
	serviceURL := cn.getServiceURL()

	containerURL = serviceURL.NewContainerURL(cn.azcontainer)
	return containerURL
}

func (cn *Conn) getBlockBlobURL(blobname string) (azblob.BlockBlobURL) {
	var blockBlobURL azblob.BlockBlobURL
	containerURL := cn.getContainerURL()

	blockBlobURL = containerURL.NewBlockBlobURL(blobname)
	return blockBlobURL
}

func (cn *Conn) getBlobURL(blobname string) (azblob.BlobURL) {
	var blobURL azblob.BlobURL
	containerURL := cn.getContainerURL()

	blobURL = containerURL.NewBlobURL(blobname)
	return blobURL
}

func (cn *Conn) uploadFile(absfilepath string, relfilepath string, uniqueid string, parallelism uint) (error){
	// Upload the file to a block blob
	blockBlobURL := cn.getBlockBlobURL(uniqueid+"/"+relfilepath)
	file, err := os.Open(absfilepath)
	handleErrors("Error in opening backup file",err)

	_, err = azblob.UploadFileToBlockBlob(context.Background(), file, blockBlobURL,
						azblob.UploadToBlockBlobOptions{
						Parallelism: uint16(parallelism),
			})

	return err
}

func (cn *Conn) downloadFile(outdir string, uniqueid string, blobpath string, parallelism uint) {
	for marker := (azblob.Marker{}); marker.NotDone(); {
		containerURL := cn.getContainerURL()
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(context.Background(), marker, azblob.ListBlobsSegmentOptions{})
		handleErrors("Error in listing segment of blobs",err)

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker
		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			if strings.HasPrefix(blobInfo.Name, blobpath) {

				log.Println("Downloading file :", blobInfo.Name)
				// Set up file to download the blob to
				dir, filename := filepath.Split(blobInfo.Name)

				relfilepath, err := filepath.Rel(uniqueid,dir)
				handleErrors("Error in fetching download relative path",err)

				dumpdir := outdir+"/"+relfilepath
				err = os.MkdirAll(dumpdir, 0777)
				handleErrors("Error in creating backup directory structure on NPS/PDA",err)

				outfilepath := path.Join(dumpdir, filename)
				filehandle, err := os.Create(outfilepath)
				handleErrors("Error in creating file inside backup dir on PDA/NPS",err)

				defer filehandle.Close()

				blobURL := cn.getBlobURL(blobInfo.Name)

				// Perform download
				err = azblob.DownloadBlobToFile(context.Background(), blobURL, 0, 0, filehandle, 
									azblob.DownloadFromBlobOptions{ 
									Parallelism: uint16(parallelism),
						})
				handleErrors("Error in downloading an Azure blob to a file",err)
			}
		}
	}
}

func main() {
	var conn Conn
	var backupinfo BackupInfo
	var othargs OtherArgs

	// parse input args
	parseArgs(&conn, &backupinfo, &othargs)
	flag.Parse()

	// log file configuration setup
	logfilename := fmt.Sprintf("nz_azconnector_%d_%s.log", os.Getppid(), time.Now().Format("2006-01-02"))
	logfilepath := path.Join(othargs.logfiledir, logfilename)
	filehandle, err := os.OpenFile(logfilepath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	handleErrors("Error in opening logfile",err)
	log.SetOutput(filehandle)
	prefixStr := fmt.Sprintf("%s  ", time.Now().UTC().Format("2006-01-02 15:04:05 EST")) + fmt.Sprintf("%-7s", "[INFO]")
	log.SetFlags(0)
	log.SetPrefix(prefixStr)

	dirlist := strings.Split(backupinfo.dirs," ")
	log.Println("Azure account name :", conn.azaccount)
	log.Println("Azure key :", conn.azkey)
	log.Println("Azure container :", conn.azcontainer)
	log.Println("Number of blocks to upload/download in parallel :", conn.parallelism)
	log.Println("Backup/Restore directory :",dirlist)
	log.Println("DB name :", backupinfo.dbname)
	log.Println("Nps hostname :", backupinfo.npshost)
	log.Println("BackupsetID :", backupinfo.backupsetID)
	log.Println("UniqueID :", othargs.uniqueid)

	for _, bkpdir := range dirlist {
		if (*othargs.upload) {
			log.Println("Uploading backup data to azure cloud from backup dir", bkpdir)
			backupdir := fmt.Sprintf("%s/%s/%s/%s/%s", bkpdir, "Netezza", backupinfo.npshost, backupinfo.dbname, backupinfo.backupsetID)
			err := filepath.Walk(backupdir,
				func(absfilepath string, info os.FileInfo, err error) error {
				if !info.IsDir() {
					relfilepath, err := filepath.Rel(bkpdir,absfilepath)
					handleErrors("Error in traversing backup directory structure",err)

					log.Println("Uploading file :", absfilepath)
					err = conn.uploadFile(absfilepath, relfilepath, othargs.uniqueid, conn.parallelism)
					return err
				} else {
					return nil
				}
			})
			handleErrors("Error in uploading file",err)
		}

		if (*othargs.download) {
			log.Println("Downloading backup data from azure cloud to restore dir", bkpdir)
			blobpath := fmt.Sprintf("%s/%s/%s/%s/%s", othargs.uniqueid, "Netezza", backupinfo.npshost, backupinfo.dbname, backupinfo.backupsetID)
			conn.downloadFile(bkpdir, othargs.uniqueid, blobpath, conn.parallelism)
		}
	}
}