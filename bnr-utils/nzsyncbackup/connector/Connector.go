package Connector

import (
        "log"
        "fmt"
        "time"
        "io"
        "io/ioutil"
        "os"
        "path"
        "strings"
        "github.com/spf13/cobra"
)

type IConnector interface {
    Upload(*OtherArgs, *BackupInfo) (error)
    Download( *OtherArgs, *BackupInfo) (error)
}

type BackupInfo struct {
    dbname      string
    Dir         string
    npshost     string
    backupset   string
    increment   string
}

type OtherArgs struct {
    uniqueid    string
    logfiledir  string
    paralleljobs int
    Operation   int
    upload *bool
    download *bool
    Connector   string
}

func ParseArgs(backupinfo *BackupInfo, otherargs *OtherArgs, azconnect *AZConnector, s3connect *S3connector) {
    var cmdAws = &cobra.Command{
        Use : "aws",
        Short: "Upload/Download Backup to/from AWS Cloud",
        Run: func(cmd *cobra.Command, args []string) {
            otherargs.Connector = "aws"
        },
    }
    var cmdAzure = &cobra.Command{
        Use : "azure",
        Short: "Upload/Download Backup to/From  Azure Cloud",
        Run: func(cmd *cobra.Command, args []string) {
            otherargs.Connector = "azure"
        },
    }


    var rootCmd = &cobra.Command{Use: "nzsyncbackup"}


    rootCmd.PersistentFlags().StringVar(&backupinfo.dbname, "db", "", "Database name")
    rootCmd.PersistentFlags().StringVar(&backupinfo.Dir, "dir", "", "Full path to the directory in which the backup already exists or should be downloaded")
    rootCmd.PersistentFlags().StringVar(&backupinfo.npshost, "npshost", "", "Name of the NPS host as it appears in the backups")
    rootCmd.PersistentFlags().StringVar(&backupinfo.backupset, "backupset", "", "Name of the backupset to be uploaded/downloaded")
    rootCmd.PersistentFlags().StringVar(&backupinfo.increment, "increment", "", "Increment Number to be uploaded/downloaded")
    rootCmd.PersistentFlags().StringVar(&otherargs.uniqueid,"uniqueid", "", "Azure blob storage container")
    rootCmd.PersistentFlags().StringVar(&otherargs.logfiledir,"logfiledir", "/tmp", "Logfile directory for this utility. Default is /tmp dir")
    otherargs.upload = rootCmd.PersistentFlags().Bool("upload", false, "Upload Backup to Cloud")
    otherargs.download = rootCmd.PersistentFlags().Bool("download", false, "Download backup from cloud")
    rootCmd.PersistentFlags().IntVar(&otherargs.paralleljobs,"paralleljobs",6,"Number of parallel files to upload/download")




    cmdAws.Flags().StringVar(&s3connect.Access_key_id, "access-key", "", "The access key for the object store [AWS_ACCESS_KEY_ID] (required)")
    cmdAws.Flags().StringVar(&s3connect.Bucket_url, "bucket-url", "", "The bucket url to store backups to (required)")
    cmdAws.Flags().StringVar(&s3connect.Default_region, "region", "", "The region of the object store bucket (required)")
    cmdAws.Flags().StringVar(&s3connect.Secret_access_key, "secret-key", "", "The secret key for the object store [AWS_SECRET_ACCESS_KEY] (required)")
    cmdAws.Flags().StringVar(&s3connect.Endpoint,  "endpoint", "", "The endpoint for object to be store on IBM cloud")
    cmdAws.Flags().IntVar(&s3connect.Streams, "streams", 16, "Number of blocks to upload/download in parallel default 16")
    cmdAws.Flags().Int64Var(&s3connect.Blocksize, "blocksize", 100, "Block size in MB to upload/download file")

    cmdAzure.Flags().StringVar(&azconnect.Azkey, "account-key", "", "The Azure Blob account key (required)")
    cmdAzure.Flags().StringVar(&azconnect.Azaccount, "account-name", "", "The Azure Blob account name (required)")
    cmdAzure.Flags().UintVar(&azconnect.Streams, "streams", 16, "Number of blocks to upload/download in parallel default 16")
    cmdAzure.Flags().Int64Var(&azconnect.Blocksize, "blocksize", 100, "Block size in MB to upload/download file")
    cmdAzure.Flags().StringVar(&azconnect.Azcontainer, "container", "", "The Azure Blob container name (required)")
    cmdAws.MarkFlagRequired("region")
    cmdAws.MarkFlagRequired("access-key")
    cmdAws.MarkFlagRequired("bucket-name")
    cmdAws.MarkFlagRequired("secret-key")
    cmdAzure.MarkFlagRequired("account-key")
    cmdAzure.MarkFlagRequired("account-name")
    cmdAzure.MarkFlagRequired("container")
    rootCmd.AddCommand(cmdAws, cmdAzure)
    
    if err := rootCmd.Execute(); err != nil {
        log.Println(err)
        os.Exit(1)
    }
}

func SetUpLogFile(backupinfo *BackupInfo, otherargs *OtherArgs) {
   // log file configuration setup
    logfilename := fmt.Sprintf("nz_%sConnector_%d_%s.log", otherargs.Connector, os.Getppid(), time.Now().Format("2006-01-02-150405"))
    logfilepath := path.Join(otherargs.logfiledir, logfilename)
    filehandle, err := os.OpenFile(logfilepath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
    if err != nil {
        fmt.Errorf("Error in opening logfile: %v",err)
    }
    w := io.MultiWriter(os.Stdout, filehandle)
    log.SetOutput(w)
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

func SetOperation(otherargs *OtherArgs) {
    if (*otherargs.upload){
       otherargs.Operation = 0
    }
    if (*otherargs.download){
        otherargs.Operation = 1
    }
}

func updateLocation(arrLoc []string,outdir string){
    for _,locFile := range arrLoc{
        input, err := ioutil.ReadFile(locFile)
        if err != nil {
            log.Fatalf("Unable to open %s to read: %v\n", locFile, err)
        }
        lines := strings.Split(string(input), "\n")
        if (len(lines) == 2 && !strings.HasSuffix(lines[len(lines) -2] , outdir)) {
            f, err := os.OpenFile(locFile,
                os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
            if err != nil {
                log.Fatalf("Unable to open %s for update: %v\n", locFile, err)
            }
            defer f.Close()
            textAppend := "1,1,1," + outdir + "\n"
            if _, err := f.WriteString(textAppend); err != nil {
                log.Fatalf("Unable to update %s: %v\n", locFile, err)
            }
        }
    }
}

func updateContents(arrContents []string){
    for _,contentFile := range arrContents{
        input, err := ioutil.ReadFile(contentFile)
        if err != nil {
            log.Fatalln("Unable to open %s to read: %v\n",contentFile,err)
        }

        lines := strings.Split(string(input), "\n")
        var textline []string
        for i := 0 ; i < len(lines) ; i++ {
            line := lines[i]
            token := strings.Split(line, ",")
            if ( token[len(token)-1] == "0" ) {
                token[len(token)-1] = "1"
            }
            textline = append(textline, strings.Join(token, ","))
        }
        output := strings.Join(textline, "\n")
        err = ioutil.WriteFile(contentFile, []byte(output), 0644)
        if err != nil {
            log.Fatalln("Unable to update %s: %v\n",contentFile,err)
        }
    }
}

