package main

import (
    "log"
    "nzsyncbackup/connector"
    "nzsyncbackup/factory"
)

const(
    upload = iota
    download
)


func main() {
    var backupinfo Connector.BackupInfo
    var otherargs Connector.OtherArgs
    var azconnect Connector.AZConnector
    var s3connect Connector.S3connector
    var err error
// parse input args
    Connector.ParseArgs(&backupinfo, &otherargs, &azconnect, &s3connect)
    connector := Factory.GetConnector(otherargs.Connector, &azconnect, &s3connect)

    if ( connector != nil ) {
    Connector.SetOperation(&otherargs)
    log.Println("OPeration is ", otherargs.Operation)
    switch otherargs.Operation {
            case upload:
                Connector.SetUpLogFile(&backupinfo, &otherargs)
                // now do the upload
                log.Println("Uploading backup data to cloud for conector : ",otherargs.Connector)
                err = connector.Upload(&otherargs, &backupinfo)
                if (err != nil) {
                    log.Fatalln(err)
                }
                log.Println("Upload successful")
            case download:
                Connector.SetUpLogFile(&backupinfo, &otherargs)
                log.Println("Downloading backup data from cloud for connector : ", otherargs.Connector)
                err = connector.Download(&otherargs, &backupinfo)
                if (err != nil) {
                    log.Fatalln(err)
                }
                log.Println("Download successful")
            default:
                log.Fatalln("Invalid Operation, Supported  Upload/Download")
        }
    }
}
