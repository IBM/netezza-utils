package main

import (
    "flag"
    "nzconnector/connector"
    "nzconnector/factory"
    "log"
    "strings"
)

func parseConnectorArgs(e Connector.IConnector, args string) {
    if e != nil {
        e.ParseConnectorArgs(args)
    }
}


func main() {
    var backupinfo Connector.BackupInfo
    var connectorInfo Connector.ConnectorInfo
    var otherargs Connector.OtherArgs
    var err error
// parse input args
    Connector.ParseArgs(&backupinfo, &connectorInfo, &otherargs)
    flag.Parse()
    Connector.SetUpLogFile(&backupinfo, &connectorInfo, &otherargs)
    connector := Factory.GetConnector(connectorInfo.Connector)
    parseConnectorArgs(connector, connectorInfo.ConnectorArgs)

    dirlist := strings.Split(backupinfo.Dir," ")
    for _, bkpdir := range dirlist {
        if (*otherargs.Upload) {
            // now do the upload
            log.Println("Uploading backup data to cloud from backup dir", bkpdir)
            err = connector.UploadBkp(bkpdir, &otherargs, &backupinfo)
            if (err != nil) {
                log.Fatalln(err)
            }
            log.Println("Upload successful")
        }
        if (*otherargs.Download) {
            log.Println("Downloading backup data from cloud to restore dir", bkpdir)
            err = connector.DownloadBkp(bkpdir, &otherargs, &backupinfo)
            if (err != nil) {
                log.Fatalln(err)
            }
            log.Println("Download successful")
        }
    }
}
