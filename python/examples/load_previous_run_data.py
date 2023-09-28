import os
import tempfile

from sparktestingbase.sqltestcase import SQLTestCase
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import Row
from .dual_write import DualWriteExample


class LoadPreviousRunData(SQLTestCase):

    def find_oldest_id(self, local_path):
        directories = os.listdir()
        return directories.sort(key=os.path.getmtime)[0]
    
    def load_json_records(self):
        local_path = "/tmp/spark-events"
        event_log_path = f"file:/{local_path}"
        application_id = find_oldest_id(local_path)
        print(f"Loading {application_id}")
        full_log_path = f"{event_log_path}/{application_id}"
        df = self.sqlCtx.load.json(full_log_path)
        special_events = df.filter(df("Event") == "SparkListenerExecutorAdded" || df("Event") == "SparkListenerJobEnd")
        special_events.show()
