package Connector

import (
    "fmt"
    "strings"
)

type is3 interface {
    s3args()
}

type S3connector struct {
    access_key_id string
    secret_access_key string
    default_region string
    bucket_url string
    endpoint string
    uniqueid string
}

func (c *S3connector) Upload() {
    c.s3args()
    fmt.Println("Uploading with s3 connector")
}

func (c *S3connector) UploadBkp(bkpdir string, uniqueid string, backupdir string, paralleljobs int) (error){
    fmt.Println("Uploading")
    return nil
}
func (cn *S3connector) DownloadBkp(outdir string, uniqueid string, blobpath string, paralleljobs int) (error){
    fmt.Println("Downloading")
    return nil
}
func (c *S3connector) ParseConnectorArgs(args string) {
    arguments := strings.Split(args, ":")
    for _, arg := range arguments {
        kv := strings.Split(arg, "=")
        switch kv[0] {
        case "ACCESS_KEY_ID":
            c.access_key_id = kv[1]
        case "SECRET_ACCESS_KEY":
            c.secret_access_key = kv[1]
        case "DEFAULT_REGION":
            c.default_region = kv[1]
        case "BUCKET_URL":
            c.bucket_url = kv[1]
        case "ENDPOINT":
            c.endpoint = kv[1]
        }
    }
}

func (c S3connector) s3args() {
    fmt.Println("ACCESS_KEY_ID : ", c.access_key_id)
}

