# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import notebookutils 

variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

lh_workspace_name = variables.LH_WORKSPACE_NAME

bronze_lh_name = variables.BRONZE_LH_NAME


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE_LH_SCHEMAS_TO_CREATE = ["youtube"] 

for schema_name in BRONZE_LH_SCHEMAS_TO_CREATE: 
    
    # create a dynmamic Spark SQL script, reading from the Variable Library variables, and the BRONZE_LH_SCHEMAS_TO_CREATE metadata
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{lh_workspace_name}`.`{bronze_lh_name}`.`{schema_name}`")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE_TABLE_SCHEMAS = {
    "youtube.channel": """
        channel_id STRING, 
        channel_name STRING, 
        channel_description STRING, 
        view_count INT, 
        subscriber_count INT, 
        video_count INT, 
        loading_TS TIMESTAMP""",
    "youtube.playlist_items": """
        channel_id STRING, 
        video_id STRING, 
        video_title STRING, 
        video_description STRING,
        thumbnail_url STRING,
        video_publish_TS TIMESTAMP,
        loading_TS TIMESTAMP""",
    "youtube.videos": """
        video_id STRING, 
        video_view_count INT, 
        video_like_count INT, 
        video_comment_count INT,
        loading_TS TIMESTAMP""", 
}

# for each key,value in the metadata object BRONZE_TABLE_SCHEMAS 
for table, ddl in BRONZE_TABLE_SCHEMAS.items(): 
    
    # create a dynmamic Spark SQL script, reading from the Variable Library variables, and the BRONZE_TABLE_SCHEMAS metadata
    # we use IF NOT EXISTS to avoid overwrite - all updates will be done through migrations
    create_script = f"CREATE TABLE IF NOT EXISTS `{lh_workspace_name}`.`{bronze_lh_name}`.{table} ({ddl});" 
    
    # run the SQL statement to create the table
    spark.sql(create_script)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
