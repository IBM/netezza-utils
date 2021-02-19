package Connector

import (
        "flag"
        "log"
        "fmt"
        "time"
        "os"
        "path"
)

type IConnector interface {
    ParseConnectorArgs(string)
    UploadBkp(string, *OtherArgs, *BackupInfo) (error)
    DownloadBkp(string, *OtherArgs, *BackupInfo) (error)
}

type BackupInfo struct {
    dbname      string
    Dir         string
    npshost     string
    backupset   string
}

type ConnectorInfo struct {
    Connector   string
    ConnectorArgs   string
}

type OtherArgs struct {
    uniqueid    string
    logfiledir  string
    Upload      *bool
    Download    *bool
    paralleljobs int
}

func ParseArgs(backupinfo *BackupInfo, connectorInfo *ConnectorInfo, otherargs *OtherArgs) {
    flag.StringVar(&backupinfo.dbname, "db", "", "Database name")
    flag.StringVar(&backupinfo.Dir, "dir", "", "Full path to the directory in which the backup already exists or should be downloaded")
    flag.StringVar(&backupinfo.npshost, "npshost", "", "Name of the NPS host as it appears in the backups")
    flag.StringVar(&backupinfo.backupset, "backupset", "", "Name of the backupset to be uploaded/downloaded")

    flag.StringVar(&connectorInfo.Connector, "connector", "", "Destination cloud store")
    flag.StringVar(&connectorInfo.ConnectorArgs, "connectorArgs", "", "Arguments for cloud store")

    flag.StringVar(&otherargs.uniqueid,"uniqueid", "", "Azure blob storage container")
    flag.StringVar(&otherargs.logfiledir,"logfiledir", "/tmp", "Logfile directory for this utility. Default is /tmp dir")
    otherargs.Upload = flag.Bool("upload", false, "Upload to cloud")
    otherargs.Download = flag.Bool("download", false, "Download from cloud")
    flag.IntVar(&otherargs.paralleljobs,"paralleljobs",6,"Number of parallel files to upload/download")
}

func SetUpLogFile(backupinfo *BackupInfo, connectorInfo *ConnectorInfo, otherargs *OtherArgs) {
   // log file configuration setup
    logfilename := fmt.Sprintf("nz_%sConnector_%d_%s.log", connectorInfo.Connector, os.Getppid(), time.Now().Format("2006-01-02-150405"))
    logfilepath := path.Join(otherargs.logfiledir, logfilename)
    filehandle, err := os.OpenFile(logfilepath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
    if err != nil {
        fmt.Errorf("Error in opening logfile: %v",err)
    }
    log.SetOutput(filehandle)
    prefixStr := fmt.Sprintf("%s  ", time.Now().UTC().Format("2006-01-02 15:04:05 EST")) + fmt.Sprintf("%-7s", "[INFO]")
    log.SetFlags(0)
    log.SetPrefix(prefixStr)

    log.Println("Backup/Restore directory :",backupinfo.Dir)
    log.Println("DB name :", backupinfo.dbname)
    log.Println("Nps hostname :", backupinfo.npshost)
    log.Println("BackupsetID :", backupinfo.backupset)
    log.Println("UniqueID :", otherargs.uniqueid)
    log.Println("Number of files to upload/download in parallel :", otherargs.paralleljobs)
} 
