from pyspark.sql.session import SparkSession
import os
import tempfile

from sparktestingbase.sqltestcase import SQLTestCase
from .load_previous_run_data import LoadPreviousRunData


class TestLoadPreviousRunData(SQLTestCase):
    def test_do_magic(self):
        lprd = LoadPreviousRunData(SparkSession._getActiveSessionOrCreate())
        lprd.do_magic()
        assert False