from pyspark import SparkFiles
from pyspark.sql import *
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
)

spark = SparkSession.builder.master("local[4]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# tag::global_setup[]
se_conf = {
    "se_notifications_enable_email": False,
    "se_notifications_email_smtp_host": "mailhost.example.com",
    "se_notifications_email_smtp_port": 25,
    "se_notifications_email_from": "timbit@example.com",
    "se_notifications_email_subject": "spark expectations - data quality - notifications",
    "se_notifications_on_fail": True,
    "se_notifications_on_error_drop_exceeds_threshold_breach": True,
    "se_notifications_on_error_drop_threshold": 15,
}
# end::gloabl_setup[]


# tag::setup_and_load[]
from spark_expectations.config.user_config import Constants as user_config

spark.sql("DROP TABLE IF EXISTS local.magic_validation")
spark.sql(
    """
create table local.magic_validation (
    product_id STRING,
    table_name STRING,
    rule_type STRING,
    rule STRING,
    column_name STRING,
    expectation STRING,
    action_if_failed STRING,
    tag STRING,
    description STRING,
    enable_for_source_dq_validation BOOLEAN,
    enable_for_target_dq_validation BOOLEAN,
    is_active BOOLEAN,
    enable_error_drop_alert BOOLEAN,
    error_drop_threshold INT
)"""
)
# Reminder: addFile does not handle directories well.
rule_file = "spark_expectations_sample_rules.json"
sc.addFile(rule_file)
df = spark.read.json(SparkFiles.get(rule_file))
print(df)
df.write.option("byname", "true").mode("append").saveAsTable("local.magic_validation")
spark.read.table("local.magic_validation").show()

# Can be used to point to your desired metastore.
se_writer = WrappedDataFrameWriter().mode("append").format("iceberg")

rule_df = spark.sql("select * from local.magic_validation")

se: SparkExpectations = SparkExpectations(
    rules_df=rule_df,  # See if we can replace this with the DF we wrote out.
    product_id="pay",  # We will only apply rules matching this product id
    stats_table="local.dq_stats",
    stats_table_writer=se_writer,
    target_and_error_table_writer=se_writer,
    stats_streaming_options={user_config.se_enable_streaming: False},
)
# end::setup_and_load[]
rule_df.show(truncate=200)


# tag::run_validation_row[]
@se.with_expectations(
    user_conf=se_conf,
    write_to_table=False,  # If set to true SE will write to the target table.
    target_and_error_table_writer=se_writer,
    # target_table is used to create the error table (e.g. here local.fake_table_name_error)
    # and filter the rules on top of the global product filter.
    target_table="local.fake_table_name",
)
def load_data():
    raw_df = spark.read.csv("data/fetched/2021", header=True, inferSchema=True)
    uk_df = raw_df.select("CompanyNumber", "MaleBonusPercent", "FemaleBonuspercent")
    return uk_df


# data = load_data()
# end::run_validation_row[]


# tag::run_validation_complex[]
@se.with_expectations(
    user_conf=se_conf,
    write_to_table=True,  # If set to true SE will write to the target table.
    target_and_error_table_writer=se_writer,
    # target_table is used to create the error table (e.g. here local.fake_table_name_error)
    # and filter the rules on top of the global product filter.
    target_table="local.3rd_fake",
)
def load_data2():
    raw_df = spark.read.csv("data/fetched/2021", header=True, inferSchema=True)
    uk_df = raw_df.select("CompanyNumber", "MaleBonusPercent", "FemaleBonuspercent")
    return uk_df


data = load_data2()
# end::run_validation_complex[]

spark.sql("SELECT table_name, error_percentage, * FROM local.dq_stats").show(
    truncate=300
)
