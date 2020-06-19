import os
import tempfile
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.ssh_hook import SSHHook


class GCSToSFTPOperator(BaseOperator):
    """
    This operator is used to upload files in a Google Cloud Storage Bucket to an SFTP server location.

    :param sftp_conn_id: Connection Id in Airflow for the SFTP server .
    :param sftp_folder_path: Folder path on SFTP server where files are to be uploaded to.
    :param source_bucket: Cloud storage bucket where source files are located.
    :param source_object: Name of file in bucket to be uploaded or the prefix with wildcard to upload
                        all files with that prefix. E.g. data/files/* to upload all files with prefix
                        `data/files/`.
    :param gcs_conn_id: Connection in Airflow for the Google Cloud Storage connection.
    :param delegate_to: The account to impersonate, if any.
                    For this to work, the service account making the request must have
                    domain-wide delegation enabled.
    :param reload_all: Flag to specify if files that already exist in SFTP Path should be re-uploaded.
    """

    template_fields = ["sftp_folder_path", "source_bucket", "source_object"]
    template_ext = []
    ui_color = "#f0eee4"

    @apply_defaults
    def __init__(
        self,
        sftp_conn_id: str,
        sftp_folder_path: str,
        source_bucket: str,
        source_object: str = None,
        gcs_conn_id: str = "google_cloud_default",
        delegate_to: str = None,
        reload_all: bool = False,
        *args,
        **kwargs,
    ):
        super(GCSToSFTPOperator, self).__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_folder_path = sftp_folder_path
        self.source_bucket = source_bucket
        self.source_object = source_object
        self.gcs_conn_id = gcs_conn_id
        self.delegate_to = delegate_to
        self.reload_all = reload_all
        self.wildcard = "*"

    def execute(self, context):
        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.gcs_conn_id, delegate_to=self.delegate_to
        )
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        ssh_client = ssh_hook.get_conn()
        sftp_client = ssh_client.open_sftp()

        # Get list of files in the bucket
        if self.wildcard in self.source_object:
            prefix, delimiter = self.source_object.split(self.wildcard, 1)
            bucket_files = [
                file
                for file in gcs_hook.list(
                    bucket=self.source_bucket, prefix=prefix, delimiter=delimiter
                )
                if file != prefix
            ]
        else:
            prefix = (
                os.path.dirname(self.source_object) + "/"
            )  # add the front slash as dirname removes it
            bucket_files = [self.source_object]

        # Get list of files in sftp_path
        try:
            self.log.info(
                f"Getting list of files in sftp_path: `{self.sftp_folder_path}`"
            )
            sftp_files = sftp_client.listdir(self.sftp_folder_path)
        except IOError as e:
            self.log.error(
                f"The folder `{self.sftp_folder_path}` does not exist on the sftp server."
            )
            raise e

        # determine the files to be processed. If all files are to be reloaded then process all files
        # in the bucket file list. If all files are not to be reloaded then only process files for
        # which the file name does not currently exist in the sftp folder
        if self.reload_all:
            files_to_process = {
                filename.replace(prefix, "") for filename in bucket_files
            }
            self.log.info(f"Files to process: `{files_to_process}`")
        else:
            existing_set = {filename.replace(prefix, "") for filename in bucket_files}
            files_to_process = existing_set - set(sftp_files)
            self.log.info(f"Existing files in sftp folder: `{set(sftp_files)}`")
            self.log.info(f"Files to process: `{files_to_process}`")

        # create temporary folder and process files
        with tempfile.TemporaryDirectory() as temp_folder:
            for file in files_to_process:
                temp_path = os.path.join(temp_folder, file)
                gcs_object = os.path.join(prefix, file)
                sftp_object = os.path.join(self.sftp_folder_path, file)

                self.log.info(f"Processing file: `{gcs_object}`")
                gcs_hook.download(
                    bucket=self.source_bucket, object=gcs_object, filename=temp_path
                )
                sftp_client.put(localpath=temp_path, remotepath=sftp_object)
                os.remove(temp_path)
