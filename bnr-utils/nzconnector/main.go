package main

import (
    "flag"
    "nzconnector/connector"
    "fmt"
    "path/filepath"
    "log"
    "os"
    "time"
    "path"
    "strings"
)

type BackupInfo struct {
    dbname      string
    dir         string
    npshost     string
    backupset   string
}

type ConnectorInfo struct {
    connector   string
    connectorArgs   string
}

type OtherArgs struct {
    uniqueid    string
    logfiledir  string
    upload      *bool
    download    *bool
    paralleljobs int
}


func parseArgs(backupinfo *BackupInfo, connectorInfo *ConnectorInfo, otherargs *OtherArgs) {
    flag.StringVar(&backupinfo.dbname, "db", "", "Database name")
    flag.StringVar(&backupinfo.dir, "dir", "", "Full path to the directory in which the backup already exists or should be downloaded")
    flag.StringVar(&backupinfo.npshost, "npshost", "", "Name of the NPS host as it appears in the backups")
    flag.StringVar(&backupinfo.backupset, "backupset", "", "Name of the backupset to be uploaded/downloaded")
    
    flag.StringVar(&connectorInfo.connector, "connector", "", "Destination cloud store")
    flag.StringVar(&connectorInfo.connectorArgs, "connectorArgs", "", "Arguments for cloud store")
    
    flag.StringVar(&otherargs.uniqueid,"uniqueid", "", "Azure blob storage container")
    flag.StringVar(&otherargs.logfiledir,"logfiledir", "/tmp", "Logfile directory for this utility. Default is /tmp dir")
    otherargs.upload = flag.Bool("upload", false, "Upload to cloud")
    otherargs.download = flag.Bool("download", false, "Download from cloud")
    flag.IntVar(&otherargs.paralleljobs,"paralleljobs",6,"Number of parallel files to upload/download")
}

func handleErrors(err error) {
    if err != nil {
        log.Fatalln(err)
    }
}

func parseConnectorArgs(e Connector.IConnector, args string) {
    if e != nil {
        e.ParseConnectorArgs(args)
    }
}

func GetConnector(connectorType string) Connector.IConnector {
    switch connectorType {
    case "s3":
        return &Connector.S3connector{}
    case "az":
        return &Connector.AZConnector{}
    default:
        return nil
    }
}

func main() {
    var backupinfo BackupInfo
    var connectorInfo ConnectorInfo
    var otherargs OtherArgs
    // parse input args
    parseArgs(&backupinfo, &connectorInfo, &otherargs)
    flag.Parse()

    connector := GetConnector(connectorInfo.connector)
    parseConnectorArgs(connector, connectorInfo.connectorArgs)

    // log file configuration setup
    logfilename := fmt.Sprintf("nz_azConnector_%d_%s.log", os.Getppid(), time.Now().Format("2006-01-02"))
    logfilepath := path.Join(otherargs.logfiledir, logfilename)
    filehandle, err := os.OpenFile(logfilepath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
    if err != nil {
        fmt.Errorf("Error in opening logfile: %v",err)
    }
    log.SetOutput(filehandle)
    prefixStr := fmt.Sprintf("%s  ", time.Now().UTC().Format("2006-01-02 15:04:05 EST")) + fmt.Sprintf("%-7s", "[INFO]")
    log.SetFlags(0)
    log.SetPrefix(prefixStr)

    dirlist := strings.Split(backupinfo.dir," ")
    log.Println("Backup/Restore directory :",dirlist)
    log.Println("DB name :", backupinfo.dbname)
    log.Println("Nps hostname :", backupinfo.npshost)
    log.Println("BackupsetID :", backupinfo.backupset)
    log.Println("UniqueID :", otherargs.uniqueid)
    log.Println("Number of files to upload/download in parallel :", otherargs.paralleljobs)

    for _, bkpdir := range dirlist {
        if (*otherargs.upload) {

            // now do the upload
            log.Println("Uploading backup data to azure cloud from backup dir", bkpdir)
            backupdir := filepath.Join(bkpdir, "Netezza", backupinfo.npshost, backupinfo.dbname, backupinfo.backupset)
            _, err = os.Stat(backupdir)
            handleErrors(err)
            err = connector.UploadBkp(bkpdir, otherargs.uniqueid, backupdir, otherargs.paralleljobs)
            handleErrors(err)
            log.Println("Upload successful")
        }
        if (*otherargs.download) {
            log.Println("Downloading backup data from azure cloud to restore dir", bkpdir)
            blobpath := filepath.Join(otherargs.uniqueid, "Netezza",backupinfo.npshost, backupinfo.dbname, backupinfo.backupset)
            err = connector.DownloadBkp(bkpdir, otherargs.uniqueid, blobpath, otherargs.paralleljobs)
            handleErrors(err)
            log.Println("Download successful")
        }
    }
}
