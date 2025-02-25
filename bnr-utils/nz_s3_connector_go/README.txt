nz_s3_connector is a utility to perform upload and download of backup files between a local directory and an AWS S3 or IBM COS.

Usage
Run the script with necessary flags.

Upload 
go run nz_s3Connector.go -access-key <access-key> -secret-key <secret-key> -region <default_region> -dir /tmp/dir -db DB1 -npshost **** -unique-id <unique_id> 
-bucket-url <bucket-name> -upload -paralleljobs <no of parallel jobs>

Download
go run nz_s3Connector.go -access-key <access-key> -secret-key <secret-key> -region <default-region> -dir /tmp/dir -db <db-name> -npshost **** -unique-id <unique-id> 
-bucket-url <bucket-name> -download -paralleljobs <no of parallel jobs>

Available Parameters:
-access-key
    Access Key Id to access AWS s3/IBM cloud
-backupset
    Name of the backupset to be uploaded/downloaded. If omitted then all the files from the directory would be uploaded/downloaded
-blocksize
    Block size in MB to upload/download file (default 100)
-bucket-url
    Bucket url to access AWS s3/IBM cloud
-db
    Database name
-dir
    Full path to the directory in which the backup already exists or should be downloaded. Enclose in double quotes
    if there are multiple directories.
-download
    Download from cloud
-endpoint
    URL of the entry point for an AWS s3/IBM cloud. Mandatory for IBM cloud service.
-logfiledir
    Logfile directory for this utility. Default is /tmp dir
-npshost
    Name of the NPS host as it appears in the backups
-paralleljobs
    Parallel jobs for upload/download (default 6)
-region
    Default region of your bucket in AWS s3/IBM cloud
-secret-key
    Secret Access Key to access access AWS s3/IBM cloud
-streams
    Number of blocks to upload/download in parallel default 16 (default 16)
-unique-id
    Unique ID associated with the file transfer
-upload
    Upload from cloud