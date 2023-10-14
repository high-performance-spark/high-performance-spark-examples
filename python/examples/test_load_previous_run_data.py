from pyspark.sql.session import SparkSession
import os
import tempfile

from sparktestingbase.sqltestcase import SQLTestCase
from .load_previous_run_data import LoadPreviousRunData


class TestLoadPreviousRunData(SQLTestCase):
    def test_do_magic(self):
        lprd = LoadPreviousRunData(self.session)
        try:
            lprd.do_magic()
        except FileNotFoundError:
            print("No previous jobs")
