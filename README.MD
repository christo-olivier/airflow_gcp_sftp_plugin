# sFTP to Google Cloud Storage Plugin

This plugin provides operators allowing you to sync files between an sFTP server and Google Cloud Storage bucket.

## Hooks

No hooks are developed as the contributed GoogleCloudStorageHook and SSHHook that are part of Airflow are used for interacting with GCS and the sFTP server respectively. 

## Operators

There are currently 3 operators created as part of the plugin:
1. GCSToSFTPOperator
1. SFTPToGCSOperator
1. SFTPDeleteFileOperator

## Installation

In order to make use of the plugin copy the `gcp_sftp_plugin` package in the root of the repository into 
the `$AIRFLOW_HOME/plugins` folder. If the `$AIRFLOW_HOME/plugins` folder does not exist, then simply create it. 

Once you have completed your Airflow plugins folder should look like this

```
$AIRFLOW_HOME/plugins/gcp_sftp_plugin
$AIRFLOW_HOME/plugins/gcp_sftp_plugin/__init__.py     
$AIRFLOW_HOME/plugins/gcp_sftp_plugin/operators/__init__.py
$AIRFLOW_HOME/plugins/gcp_sftp_plugin/operators/gcs_to_sftp_operator.py
$AIRFLOW_HOME/plugins/gcp_sftp_plugin/operators/sftp_delete_file_operator.py   
$AIRFLOW_HOME/plugins/gcp_sftp_plugin/operators/sftp_to_gcs_operator.py
```

In order to use the hook and operator use the following imports:

```Python
from airflow.operators.gcp_sftp_plugin import GCSToSFTPOperator, SFTPToGCSOperator, SFTPDeleteFileOperator
```