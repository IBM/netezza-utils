nz_s3Connector

Usage:   ./nz_s3Connector [-h] -db <dbname> -dir <location1> <location2> -access-key <access-key> -secret-key <secret-key> -region <default_region>
         -unique-id <unique_id> -bucket-url <bucket-name> -npshost <hostname> -backupset <backupsetid> -streams <streams> -blocksize <blocksize> 
         -endpoint <endpoint> -paralleljobs <paralleljobs> -upload|download -logfiledir <location>

Purpose: To upload or download one or more data backup file to and from aws s3 or IBM cloud.

         An nz_s3Connector must be run locally (on the NPS host being backed up).

Options:
         -h or --help

            Display the valid flags

         -verbose

            Controls the flow of commentary. The default behaviour is that the output would be logged
            in logfile under /tmp directory. If you specify -verbose, then the relevant output would be
            shown and nothing would be logged in file.

         -db DATABASE

            The name of the database to back up

         -dir <dirname> [...]

            the full path to the directory in which the data files will be written to (or read from).
            This directory must already exist and permit write access to it.

         -access-key ACCESS_KEY_ID

            Access Key Id to access AWS s3/IBM cloud

         -secret-key SECRET_ACCESS_KEY

            Secret Access Key to access access AWS s3/IBM cloud

         -region DEFAULT_REGION

            default region of your bucket in AWS s3/IBM cloud

         -bucket-url BUCKET_NAME

            Bucket name of AWS s3/IBM cloud
         
         -endpoint ENDPOINT

            The URL of the entry point for an AWS s3/IBM cloud.
            Mandatory for IBM cloud service.

         -unique-id UNIQUE_ID

            unique ID associated with the file transfer

         -streams STREAMS

            Number of blocks to upload/download in parallel (default 16)

         -blocksize BLOCK_SIZE

            Block size in MB to upload/download file (default 100)

         -paralleljobs PARALLEL_JOBS
         
            Parallel jobs for upload/download (default 6)

         -npshost <name>

            Host name  [NZ_HOST]

         -backupset ID

            Specify a backupset ID, as displayed in the backup history report.
            If omitted then all the files from the directory would be uploaded/downloaded

         -upload|download

            Specify whether the files needs to be uploaded/downloaded to/from aws s3 or IBM cloud		
			
Examples: 

1. To upload files from npshost to aws s3/IBM cloud, you need to specify below mandatory arguments :

   o database name   : database whose backup is present on the host. In example below, db1 is
                        used as database.  
   o directory       : path under which database backup data files are present. In example below,
                     /nzscratch/db2 is used as directory.
   o connector arguments   : connector arguement such as -access-key, -secret-key, -region, -bucket-url. 
                             -endpoint is mandatory to connect to IBM cloud.
   o npshost         : nps hostname where backup data files are present.
   o upload          : to specify that you need to upload the files to cloud. 
   o backupset       : a backupset ID, as displayed in the backup history report. If omitted then all the 
	                     files from the directory would be uploaded. In example below,20191127100647 is 
	                     used as backupset.

$ ./nz_s3Connector -access-key **** -secret-key **** -region us-east-1 -dir /tmp/bkp1 -db DB1 -npshost **** 
-unique-id abhi1 -bucket-url **** -upload -paralleljobs 20 -endpoint **** -backupset 20241023114051

Outputs: 			
2025-02-26 07:27:01  [INFO] Aws S3 bucket: ****
2025-02-26 07:27:01  [INFO] Aws region: us-east-1
2025-02-26 07:27:01  [INFO] Backup/Restore directory: /tmp/bkp1
2025-02-26 07:27:01  [INFO] DB name : DB1
2025-02-26 07:27:01  [INFO] Nps hostname : ****
2025-02-26 07:27:01  [INFO] BackupsetID : 20241023114051
2025-02-26 07:27:01  [INFO] Number of files to upload/download in parallel : 20
2025-02-26 07:27:01  [INFO] Uploading data to s3 bucket **** with unique-id abhi1 from dir /tmp/bkp1/Netezza/****/DB1/20241023114051
2025-02-26 07:27:01  [INFO] File /tmp/bkp1/Netezza/****/DB1/20241023114051/1/FULL/data/data.marker uploaded successfully
2025-02-26 07:27:01  [INFO] File /tmp/bkp1/Netezza/****/DB1/20241023114051/1/FULL/md/loc1/locations.txt uploaded successfully
2025-02-26 07:27:01  [INFO] File /tmp/bkp1/Netezza/****/DB1/20241023114051/1/FULL/md/stream.0.1 uploaded successfully
2025-02-26 07:27:01  [INFO] File /tmp/bkp1/Netezza/****/DB1/20241023114051/1/FULL/md/contents.txt uploaded successfully
2025-02-26 07:27:01  [INFO] File /tmp/bkp1/Netezza/****/DB1/20241023114051/1/FULL/data/200221.full.1.1 uploaded successfully
2025-02-26 07:27:01  [INFO] File /tmp/bkp1/Netezza/****/DB1/20241023114051/1/FULL/md/schema.xml uploaded successfully
2025-02-26 07:27:01  [INFO] Total files uploaded: 6
2025-02-26 07:27:01  [INFO] Uploading complete.


2. To download files from aws s3/IBM cloud to npshost, you need to specify below mandatory arguments :

   o database name   : database whose backup is present on the host. In example below, db1 is
                        used as database.  
   o directory       : path under which database backup data files are present. In example below,
                     /tmp/bkp1 is used as directory.
   o connector arguments   : connector arguement such as -access-key, -secret-key, -region, -bucket-url. 
                             -endpoint is mandatory to connect to IBM cloud.
   o npshost         : nps hostname where backup data files are present.
   o download          : to specify that you need to download the files to cloud. 
   o backupset       : a backupset ID, as displayed in the backup history report. If omitted then all the 
	                     files from the directory would be uploaded. In example below,20191127100647 is 
	                     used as backupset.

$ $ ./nz_s3Connector -access-key **** -secret-key **** -region us-east-1 -dir /tmp/bkp1 -db DB1 -npshost **** 
-unique-id abhi1 -bucket-url **** -upload -paralleljobs 20 -endpoint **** -backupset 20241023114051

Outputs: 			
2025-02-26 07:27:21  [INFO] Aws S3 bucket: ****
2025-02-26 07:27:21  [INFO] Aws region: us-east-1
2025-02-26 07:27:21  [INFO] Backup/Restore directory: /tmp/bkp1
2025-02-26 07:27:21  [INFO] DB name : DB1
2025-02-26 07:27:21  [INFO] Nps hostname : ****
2025-02-26 07:27:21  [INFO] BackupsetID : 20241023114051
2025-02-26 07:27:21  [INFO] Number of files to upload/download in parallel : 20
2025-02-26 07:27:21  [INFO] Backup dir path: power/Netezza/****/DB1/20241023114051
2025-02-26 07:27:21  [INFO] Downloading data to dir /tmp/bkp1
2025-02-26 07:27:21  [INFO] File power/Netezza/****/DB1/20241023114051/1/FULL/md/stream.0.1 downloaded successfully
2025-02-26 07:27:21  [INFO] File power/Netezza/****/DB1/20241023114051/1/FULL/md/loc1/locations.txt downloaded successfully
2025-02-26 07:27:21  [INFO] File power/Netezza/****/DB1/20241023114051/1/FULL/data/data.marker downloaded successfully
2025-02-26 07:27:21  [INFO] File power/Netezza/****/DB1/20241023114051/1/FULL/md/contents.txt downloaded successfully
2025-02-26 07:27:21  [INFO] File power/Netezza/****/DB1/20241023114051/1/FULL/data/200221.full.1.1 downloaded successfully
2025-02-26 07:27:21  [INFO] File power/Netezza/****/DB1/20241023114051/1/FULL/md/schema.xml downloaded successfully
2025-02-26 07:27:21  [INFO] Total files downloaded: 6
2025-02-26 07:27:21  [INFO] Downloading complete.



