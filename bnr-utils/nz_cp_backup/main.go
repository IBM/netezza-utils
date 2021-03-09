package main

import (
    "flag"
    "log"
    "nzconnector/connector"
    "nzconnector/factory"
)

const(
    upload = iota
    download
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

    Connector.SetOperation(&otherargs)

    switch otherargs.Operation {
            case upload:
                // now do the upload
                log.Println("Uploading backup data to cloud for conector : ",connectorInfo.Connector)
                err = connector.Upload(&otherargs, &backupinfo)
                if (err != nil) {
                    log.Fatalln(err)
                }
                log.Println("Upload successful")
            case download:
                log.Println("Downloading backup data from cloud for connector : ", connectorInfo.Connector)
                err = connector.Download(&otherargs, &backupinfo)
                if (err != nil) {
                    log.Fatalln(err)
                }
                log.Println("Download successful")
            default:
                log.Fatalln("Invalid Operation, Supported  Upload/Download")
        }

}
