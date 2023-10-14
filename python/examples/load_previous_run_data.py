import os
import tempfile


class LoadPreviousRunData(object):
    def __init__(self, session):
        self.session = session

    def find_oldest_id(self, local_path):
        """Find the oldest Spark job since it's probably not being updated."""
        directories = os.listdir(local_path)
        return min(directories, key=lambda x: os.path.getmtime(f"{local_path}/{x}"))

    def do_magic(self):
        local_path = "/tmp/spark-events"
        event_log_path = f"file://{local_path}"
        application_id = self.find_oldest_id(local_path)
        return self.load_json_records(event_log_path, application_id)

    # tag::load[]
    def load_json_records(self, event_log_path, application_id):
        print(f"Loading {application_id}")
        full_log_path = f"{event_log_path}/{application_id}"
        df = self.session.read.json(full_log_path)
        special_events = df.filter(
            (df["Event"] == "SparkListenerExecutorAdded")
            | (df["Event"] == "SparkListenerJobEnd")
        )
        special_events.show()

    # end::load[]
