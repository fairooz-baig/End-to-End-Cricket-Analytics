from pyspark.sql.functions import *
import dlt
from pyspark.sql.types import *

#Schema
match_schema = StructType([
    StructField("id", LongType(), True),
    StructField("city", StringType(), True),
    StructField("date", StringType(), True),
    StructField("player_of_match", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("neutral_venue", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("toss_decision", StringType(), True),
    StructField("winner", StringType(), True),
    StructField("result", StringType(), True),
    StructField("result_margin", DoubleType(), True),
    StructField("eliminator", StringType(), True),
    StructField("method", StringType(), True),
    StructField("umpire1", StringType(), True),
    StructField("umpire2", StringType(), True)
])


catalog_name = spark.conf.get('catalog_name')
volume_path = f"/Volumes/{catalog_name}/bronze/cricket_data"
primary_key = "id"

#DLT

@dlt.view(name="cricket_matches_vw")
def cricket_matches_vw():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", True)
        .schema(match_schema)                    
        .load(volume_path)
        .withColumn("_load_timestamp", current_timestamp())
    )
    
    # transformations
    df = df \
        .withColumn("date", to_date(col("date"))) \
        .withColumn("result_margin", when(col("result_margin").isNull(), 0.0).otherwise(col("result_margin"))) \
        .withColumn("season", year(col("date"))) \
        .withColumn("winner_team", 
                    when(col("result") == "wickets", col("team2"))
                    .when(col("result") == "runs", col("team1"))
                    .otherwise(col("winner"))) \
        .withColumn("is_tie", when(col("result").isNull() | (col("eliminator") == "Y"), True).otherwise(False))
     
    return df

#DLT Streaming
dlt.create_streaming_table(name="cricket_data_final")

dlt.apply_changes(
    target="cricket_data_final",
    source="cricket_matches_vw",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1"          
)