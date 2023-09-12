from pyspark.sql import *
from spark_expectations_config import *

spark = SparkSession.builder.master("local[4]").getOrCreate()

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
json_added = sc.addFile("./spark_expectations_sample_rules.json")
df = spark.load(json_added)
df.save.option("byname", "true").insertInto("magic_validation")
spark.loadTable("magic_validation").show()
se: SparkExpectations = SparkExpectations(product_id="coffee", debugger=False)
#end::setup_and_load[]

#tag::run_validation[]
@se.with_expectations(
    se.reader.get_rules_from_table(
        product_rules_table="magic_validation",
        target_table_name="pay",
        dq_stats_table_name="pay_stats",
    ),
    # Stats
    write_to_table=True,
    row_dq=True,
    agg_dq={
        user_config.se_agg_dq: True,
        user_config.se_source_agg_dq: True,
        user_config.se_final_agg_dq: True,
    },
    query_dq={
        user_config.se_query_dq: True,
        user_config.se_source_query_dq: True,
        user_config.se_final_query_dq: True,
        user_config.se_target_table_view: "order",
    },
)
def load_data():
    uk_df = spark.read.csv("data/fetched/2021", header=True, inferSchema=True)
    uk_df.select("CompanyNumber", "MaleBonusPercent", "FemaleBonuspercent")
    return uk_df

#end::run_validation[]
