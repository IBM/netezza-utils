nz_s3connector

Usage:   nz_s3connector [-h] -db <dbname> -dir <location1> <location2> -connectorArgs <ACCESS_KEY_ID=:BUCKET_URL=:DEFAULT_
         REGION=:SECRET_ACCESS_KEY=:MULTIPART_SIZE_MB=:ENDPOINT=:UNIQUE_ID=> -npshost <hostname> -backupset <backupsetid>
         -upload|download -verbose

Purpose: To upload or download one or more data backup file to and from aws s3 or IBM cloud.

         An nz_s3connector must be run locally (on the NPS host being backed up).

Options:
         -h or --help

            display this help

         -verbose

            Controls the flow of commentary. The default behaviour is that the output would be logged
            in logfile under /tmp directory. If you specify -verbose, then the relevant output would be
            shown and nothing would be logged in file.

         -db DATABASE

            the name of the database to back up

         -dir <dirname> [...]

            the full path to the directory in which the data files will be written to (or read from).
            This directory must already exist and permit write access to it.

         -connectorArgs

            should be of the form "name=value[:name=value[...]]"(see usage above). Below are the arguments:

                       ACCESS_KEY_ID            Access Key Id to access AWS s3/IBM cloud
                       BUCKET_URL               bucket url to access AWS s3/IBM cloud
                       DEFAULT_REGION           default region of your bucket in AWS s3/IBM cloud
                       SECRET_ACCESS_KEY        Secret Access Key to access access AWS s3/IBM cloud
                       MULTIPART_SIZE_MB        chunk size that the CLI uses for multipart transfers of individual files.
                       ENDPOINT                 the URL of the entry point for an AWS s3/IBM cloud.
                                                Mandatory for IBM cloud service.
                       UNIQUE_ID                unique ID associated with the file transfer

         -npshost <name>

            host name  [NZ_HOST]

         -backupset ID

            specify a backupset ID, as displayed in the backup history report.
            If omitted then all the files from the directory would be uploaded/downloaded

         -upload|download

            specify whether the files needs to be uploaded/downloaded to/from aws s3 or IBM cloud

	This script would check for the aws version on the host. If aws is not present it would try and install
	aws using the aws bundled installer. This script would install AWS CLI version 1.16.291. 
	If aws is already installed on the host then it would skip this step.			
			
Examples: 

1. To upload files from npshost to aws s3/IBM cloud, you need to specify below mandatory arguments :

	 o database name	   : database whose backup is present on the host. In example below, db1 is
				     used as database.  
	 o directory    	   : path under which database backup data files are present. In example below,
				     /nzscratch/db2 is used as directory.
	 o connector arguement     : connector arguement such as ACCESS_KEY_ID, BUCKET_URL, DEFAULT_REGION,
				     SECRET_ACCESS_KEY, MULTIPART_SIZE_MB, ENDPOINT and UNIQUE_ID. ENDPOINT is mandatory
				     to connect to IBM cloud.
				     All other arguments are mandatory.
	 o npshost		   : nps hostname where backup data files are present. In example below,
				     vmnps-dw5 is used as npshost.
         o upload	           : to specify that you need to upload the files to cloud. 
	 o backupset 		   : a backupset ID, as displayed in the backup history report. If omitted then all the 
				     files from the directory would be uploaded. In example below,20191127100647 is 
				     used as backupset.

$ /nz_s3connector -db db1 -dir /nzscratch/db2 -connectorArgs "ACCESS_KEY_ID=XXX
  BUCKET_URL=XXX:DEFAULT_REGION=XXX:SECRET_ACCESS_KEY=XXX
  MULTIPART_SIZE_MB=3:ENDPOINT=XXX:UNIQUE_ID=UniqID" -npshost vmnps-dw5 -upload 
  -backupset 20191127100647 -verbose

Outputs: 			

/usr/lib/python2.6/site-packages/urllib3/util/ssl_.py:369: SNIMissingWarning: An HTTPS request has been made, but the SNI
(Server Name Indication) extension to TLS is not available on this platform. This may cause the server to present an
incorrect TLS certificate, which can cause validation failures. You can upgrade to a newer version of Python to solve this.
For more information, see https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
 SNIMissingWarning
upload: ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/contents.txt to s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/contents.txt
upload: ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/stream.0.1 to s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2
/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/stream.0.1
upload: ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/data/202370.full.1.1 to s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/data/202370.full.1.1
upload: ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/loc1/locations.txt to s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/loc1/locations.txt
upload: ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/data/202386.full.1.1 to s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/data/202386.full.1.1
upload: ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/data/data.marker to s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/data/data.marker
upload: ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/schema.xml to s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2
/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/schema.xml


2. To download files from aws s3/IBM cloud to npshost, you need to specify below mandatory arguments :

	 o database name	   : database whose backup needs to be restored on the host. In example below,
				     db1 is used as database.
	 o directory    	   : path under which database backup data files would be dumped. In example below,
				     /nzscratch/db2 is used as directory.
	 o connector arguement     : connector arguement such as ACCESS_KEY_ID, BUCKET_URL, DEFAULT_REGION, 
				     SECRET_ACCESS_KEY, MULTIPART_SIZE_MB, ENDPOINT and UNIQUE_ID. 
				     ENDPOINT is mandatory to connect to IBM cloud.
				     All other arguments are mandatory.
	 o npshost		   : nps hostname where backup data files would be dumped. In example below,
				     vmnps-dw5 is used as npshost.
         o upload		   : to specify that you need to download the files from cloud. 
	 o backupset 		   : a backupset ID, as displayed in the backup history report. If omitted then all the 
				     files from the directory would be downloaded. In example below,20191127100647
				     is used as backupset.

$ /nz_s3connector -db db1 -dir /nzscratch/db2 -connectorArgs "ACCESS_KEY_ID=XXX:BUCKET_URL=
  XXX:DEFAULT_REGION=XXX:SECRET_ACCESS_KEY=XXX:
  MULTIPART_SIZE_MB=3:ENDPOINT=XXX:UNIQUE_ID=UniqID" -npshost vmnps-dw5 -download 
  -backupset 20191127100647 -verbose

Outputs: 			
/usr/lib/python2.6/site-packages/urllib3/util/ssl_.py:369: SNIMissingWarning: An HTTPS request has been made, 
but the SNI (Server Name Indication) extension to TLS is not available on this platform. This may cause the 
server to present an incorrect TLS certificate, which can cause validation failures. You can upgrade to a newer 
version of Python to solve this. For more information, 
see https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
  SNIMissingWarning

download: s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/data
/202386.full.1.1 to ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/data/202386.full.1.1
download: s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/
loc1/locations.txt to ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/loc1/locations.txt
download: s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/
stream.0.1 to ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/stream.0.1
download: s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/
contents.txt to ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/contents.txt
download: s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/
schema.xml to ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/md/schema.xml
download: s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/data/
data.marker to ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/data/data.marker
download: s3://test.ips.bnr.aws.s3/UniqID/nzscratch/db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/data/
202370.full.1.1 to ../db2/Netezza/vmnps-dw5/DB1/20191127100647/1/FULL/data/202370.full.1.1


3. While uploading data files to aws s3/IBM cloud from npshost if you specify incorrect option say for example if 
   you provide an incorrect database name or incorrect directory or incorrect backupset then the script would throw 
   error as below.

Outputs:
   The user-provided path /nzscratch/db2/Netezza/vmnps-dw5/DB1/20191202074739 does not exist.


4. While downloading data files from aws s3/IBM cloud to npshost if you specify incorrect option say for example
   if you provide an incorrect database name or incorrect directory or incorrect backupset then the script doesn't
   throw error.
   However, your data file would not be downloaded from cloud. Script would just create the directory structure as 
   per your input data(i.e. based on database name, directory path and backupset) but there would not be any data 
   files in the directory. You would get below sample output. 

Outputs:
/usr/lib/python2.6/site-packages/urllib3/util/ssl_.py:369: SNIMissingWarning: An HTTPS request has been made, but the SNI (Server Name Indication) extension to TLS is not available on this platform. This may cause the server to present an incorrect TLS certificate, which can cause validation failures. You can upgrade to a newer version of Python to solve this. For more information, see https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
  SNIMissingWarning


