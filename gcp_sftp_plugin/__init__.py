from airflow.plugins_manager import AirflowPlugin
from gcp_sftp_plugin.operators.sftp_to_gcs_operator import SFTPToGCSOperator
from gcp_sftp_plugin.operators.gcs_to_sftp_operator import GCSToSFTPOperator
from gcp_sftp_plugin.operators.sftp_delete_file_operator import SFTPDeleteFileOperator


# Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
    name = "gcp_sftp_plugin"
    operators = [GCSToSFTPOperator, SFTPDeleteFileOperator, SFTPToGCSOperator]
