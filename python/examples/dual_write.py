import asyncactions  # noqa # pylint: disable=unused-import


class DualWriteExample:
    def do_write(self, df, p1, p2):
        """
        Apply two concrete actions to a DataFrame in parallel.
        A common use case is two views of the same data, normally
        one with sensitive data and one scrubbed/clean.
        """
        # First we "persist" it (you can also checkpoint or choose a different
        # level of persistence.
        df.persist()
        df.count()
        # Create the distinct "safe" view.
        df1 = df.select("times")
        # Start the async actions
        async1 = df1.write.mode("append").format("parquet").saveAsync(p1)
        async2 = df.write.mode("append").format("parquet").saveAsync(p2)
        # Block until the writes are both finished.
        async1.result()
        async2.result()
