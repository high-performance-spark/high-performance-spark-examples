from pyspark import SparkFiles
from pyspark.sql import *
from spark_expectations.core.expectations import SparkExpectations

spark = SparkSession.builder.master("local[4]").getOrCreate()
sc = spark.sparkContext

#tag::global_setup[]
from spark_expectations.config.user_config import *

se_global_spark_Conf = {
    "se_notifications_enable_email": False,
    "se_notifications_email_smtp_host": "mailhost.example.com",
    "se_notifications_email_smtp_port": 25,
    "se_notifications_email_from": "timbit@example.com",
    "se_notifications_email_subject": "spark expectations - data quality - notifications",
    "se_notifications_on_fail": True,
    "se_notifications_on_error_drop_exceeds_threshold_breach": True,
    "se_notifications_on_error_drop_threshold": 15,
    "se_enable_streaming": False, # Required or tries to publish to kafka.
}
#end::gloabl_setup[]


#tag::setup_and_load[]
spark.sql("DROP TABLE IF EXISTS local.magic_validation")
spark.sql("""
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
)""")
spark.sql("""
create table if not exists local.pay_stats (
    product_id STRING,
    table_name STRING,
    input_count LONG,
    error_count LONG,
    output_count LONG,
    output_percentage FLOAT,
    success_percentage FLOAT,
    error_percentage FLOAT,
    source_agg_dq_results array<map<string, string>>,
    final_agg_dq_results array<map<string, string>>,
    source_query_dq_results array<map<string, string>>,
    final_query_dq_results array<map<string, string>>,
    row_dq_res_summary array<map<string, string>>,
    row_dq_error_threshold array<map<string, string>>,
    dq_status map<string, string>,
    dq_run_time map<string, float>,
    dq_rules map<string, map<string,int>>,
    meta_dq_run_id STRING,
    meta_dq_run_date DATE,
    meta_dq_run_datetime TIMESTAMP
);""")
rule_file = "./spark_expectations_sample_rules.json"
sc.addFile(rule_file)
df = spark.read.json(SparkFiles.get(rule_file))
print(df)
df.write.option("byname", "true").mode("append").saveAsTable("local.magic_validation")
spark.read.table("local.magic_validation").show()
se: SparkExpectations = SparkExpectations(
    product_id="pay", # Used to filter which rules we apply
    debugger=True)
#end::setup_and_load[]

#tag::run_validation[]
# Only row data quality checking
@se.with_expectations(
    se.reader.get_rules_from_table(
        product_rules_table="local.magic_validation",
        target_table_name="local.bonuses",
        dq_stats_table_name="local.pay_stats",
    ),
    write_to_table=False,
    row_dq=True,
    # This does not work currently (Iceberg)
    spark_conf={"format": "iceberg"},
    options={"format": "iceberg"},
    options_error_table={"format": "iceberg"})
def load_data():
    raw_df = spark.read.csv("data/fetched/2021", header=True, inferSchema=True)
    uk_df = raw_df.select("CompanyNumber", "MaleBonusPercent", "FemaleBonuspercent")
    return uk_df

data = load_data()
#end::run_validation[]
