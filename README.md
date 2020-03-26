# PySpark Run Job

A configurable Python script to upload and run a simple PySpark job on Google Cloud Platform.
- Creates a Google Storage bucket
- Uploads a data file
- Uploads a PySpark job file
- Creates a Hadoop Cluster on Dataproc
- Creates a PySpark job
- Deletes the Hadoop Cluster
 
 ## Getting started
 ### Install GCP client libraries
 ```shell
 pip install google-cloud-storage google-cloud-dataproc
 ```

### Set up credentials for GCP
https://cloud.google.com/storage/docs/reference/libraries#setting_up_authentication

1. Follow the guide to create credentials and download them in a json file
2. Move the json file to a location you'll remember. For example `C:/big_data/`
3. Rename the file to something short and sweet e.g. `gcp_auth.json`
4. Set an environment variable with the json file location using a terminal 

***e.g. for Windows***

```shell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\big_data\gcp_auth.json"
```
***e.g. for Mac / Unix***

```shell
export GOOGLE_APPLICATION_CREDENTIALS="/home/username/big_data/gcp_auth.json"
```
Note that this variable only exists within the terminal where you executed the command. You'll need to repeat this step each time you open a new terminal. If you are working on your own laptop you can set the environment variable permanently.

 ## Running your job

 ### Settings
 Update `settings.py` with your bucket name, data file name and pyspark job filename.

 ### Run the job
 In the terminal ***(make sure that `GOOGLE_APPLICATION_CREDENTIALS` has been set)***
 ```shell
python run_job.py
 ```
 