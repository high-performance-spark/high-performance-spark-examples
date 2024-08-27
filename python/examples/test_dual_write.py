import os
import tempfile

# tag::test[]
from sparktestingbase.sqltestcase import SQLTestCase
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import Row
from .dual_write import DualWriteExample


class DualWriteTest(SQLTestCase):
    def test_always_passes(self):
        self.assertTrue(True)

    def test_actual_dual_write(self):
        tempdir = tempfile.mkdtemp()
        p1 = os.path.join(tempdir, "data1")
        p2 = os.path.join(tempdir, "data2")
        df = self.sqlCtx.createDataFrame([Row("timbit"), Row("farted")], ["names"])
        combined = df.withColumn("times", current_timestamp())
        DualWriteExample().do_write(combined, p1, p2)
        df1 = self.sqlCtx.read.format("parquet").load(p1)
        df2 = self.sqlCtx.read.format("parquet").load(p2)
        self.assertDataFrameEqual(df2.select("times"), df1, 0.1)


# end::test[]
