from pyspark.sql.session import SparkSession

# tags::pandera_imports[]
import pandera.pyspark as pa
import pyspark.sql.types as T

# end::pandera_imports[]


# tag::simple_data_schema[]
class ProjectDataSchema(pa.DataFrameModel):
    # Note str_length is currently broken :/
    creator: T.StringType() = pa.Field(str_length={"min_value": 1})
    projectname: T.StringType() = pa.Field()
    stars: T.IntegerType() = pa.Field(ge=0)


# end::simple_data_schema[]


# tag::gender_data[]
class GenderData(pa.DataFrameModel):
    MaleBonusPercent: T.DoubleType() = pa.Field(nullable=True, le=5)
    FemaleBonusPercent: T.DoubleType() = pa.Field(nullable=True)
    CompanyNumber: T.IntegerType() = pa.Field()


# end::gender_data[]

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[4]").getOrCreate()
    # Make sure to make
    # "https://gender-pay-gap.service.gov.uk/viewing/download-data/2021"
    # available as ./data/2021
    uk_df = spark.read.csv("data/fetched/2021", header=True, inferSchema=True)

    # tag::validate_gender_data[]
    validated_df = GenderData(uk_df)
    # Print out the errors. You may wish to exit with an error condition.
    if validated_df.pandera.errors != {}:
        print(validated_df.pandera.errors)
        # sys.exit(1)
    # end::validate_gender_data[]

    # tag::validate_project_data[]
    project_data = spark.read.csv("./data/project.csv", header=True, inferSchema=True)
    validated_df = ProjectDataSchema(project_data)
    # Print out the errors. You may wish to exit with an error condition.
    if validated_df.pandera.errors != {}:
        print(validated_df.pandera.errors)
        # sys.exit(1)
    # end::validate_project_data[]
