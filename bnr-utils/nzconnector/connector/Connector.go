package Connector

type IConnector interface {
    ParseConnectorArgs(string)
    UploadBkp(string, string, string, int) (error)
    Upload()
    DownloadBkp(string, string, string, int) (error)
}

