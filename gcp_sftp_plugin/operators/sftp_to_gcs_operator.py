import fnmatch
import os
import tempfile
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.ssh_hook import SSHHook


class SFTPToGCSOperator(BaseOperator):
    """
    This operator is used to download files from an SFTP server to a Google Cloud Storage Bucket.

    :param sftp_conn_id: Connection Id in Airflow for the SFTP server.
    :param sftp_folder_path: Folder path on SFTP server where files are to be downloaded from.
    :param destination_bucket: Cloud storage bucket where files are to be stored.
    :param sftp_filename: Name of the file on SFTP server to download, or unix wildcard characters to
                        specify specific or all files. If left empty all files in the folder will be
                        downloaded.
    :param destination_object_prefix: Prefix to be assigned to each file being loaded to GCS bucket.
    :param gcs_conn_id: Connection in Airflow for the Google Cloud Storage connection.
    :param delegate_to: The account to impersonate, if any.
                    For this to work, the service account making the request must have
                    domain-wide delegation enabled.
    :param reload_all: Flag to specify if files that already exist in GCS bucket should be re-downloaded.
    """

    template_fields = [
        "sftp_folder_path",
        "destination_bucket",
        "sftp_filename",
        "destination_object_prefix",
    ]
    template_ext = []
    ui_color = "#f0eee4"

    @apply_defaults
    def __init__(
        self,
        sftp_conn_id: str,
        sftp_folder_path: str,
        destination_bucket: str,
        sftp_filename: str = None,
        destination_object_prefix: str = None,
        gcs_conn_id: str = "google_cloud_default",
        delegate_to: str = None,
        reload_all: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_folder_path = sftp_folder_path
        self.sftp_filename = sftp_filename if sftp_filename else "*"
        self.destination_bucket = destination_bucket
        self.destination_object_prefix = (
            destination_object_prefix if destination_object_prefix else ""
        )
        self.gcs_conn_id = gcs_conn_id
        self.delegate_to = delegate_to
        self.reload_all = (reload_all,)

    def execute(self, context):
        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.gcs_conn_id, delegate_to=self.delegate_to
        )
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        ssh_client = ssh_hook.get_conn()
        sftp_client = ssh_client.open_sftp()

        # Get list of files in sftp_path
        self.log.info(f"Getting list of files in sftp_path: `{self.sftp_folder_path}`")
        path_content = sftp_client.listdir(self.sftp_folder_path)
        files = [
            file for file in path_content if fnmatch.fnmatch(file, self.sftp_filename)
        ]

        # Get files that already exist in the bucket
        existing_files = [
            file
            for file in gcs_hook.list(
                bucket=self.destination_bucket, prefix=self.destination_object_prefix
            )
            if file != self.destination_object_prefix
        ]

        # determine the files to be processed. If all files are to be reloaded then process all files
        # in the sftp file list. If all files are not to be reloaded then only process files for
        # which the file name does not currently exist in the GCS Bucket
        if self.reload_all:
            files_to_process = files
        else:
            existing_set = {
                filename.replace(self.destination_object_prefix, "")
                for filename in existing_files
            }
            files_to_process = set(files) - existing_set
            self.log.info(f"Existing files for bucket and prefix: `{existing_set}`")
            self.log.info(f"Files to process: `{files_to_process}`")

        # create temporary folder and process files
        with tempfile.TemporaryDirectory() as temp_folder:
            for file in files_to_process:
                temp_path = os.path.join(temp_folder, file)
                gcs_object = os.path.join(self.destination_object_prefix, file)
                sftp_object = os.path.join(self.sftp_folder_path, file)

                self.log.info(f"Processing: `{sftp_object}`")
                try:
                    sftp_client.get(sftp_object, temp_path)
                    gcs_hook.upload(
                        bucket=self.destination_bucket,
                        object=gcs_object,
                        filename=temp_path,
                    )
                    os.remove(temp_path)
                except IOError:
                    self.log.info(f"Skipping directory `{sftp_object}`.")
