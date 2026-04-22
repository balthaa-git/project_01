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

silver_lh_name = variables.SILVER_LH_NAME

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SILVER_LH_SCHEMAS_TO_CREATE = ["youtube"] 

for schema_name in SILVER_LH_SCHEMAS_TO_CREATE: 
    
    # create a dynmamic Spark SQL script, reading from the Variable Library variables, and the SILVER_LH_SCHEMAS_TO_CREATE metadata
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{lh_workspace_name}`.`{silver_lh_name}`.`{schema_name}`")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# table names from the requirements diagram
# table schemas from BRONZE tables
SILVER_TABLE_SCHEMAS = {
    "youtube.channel_stats": """
        channel_id STRING, 
        channel_name STRING, 
        channel_description STRING, 
        view_count INT, 
        subscriber_count INT, 
        video_count INT, 
        loading_TS TIMESTAMP""",
    "youtube.videos": """
        channel_id STRING, 
        video_id STRING, 
        video_title STRING, 
        video_description STRING,
        thumbnail_url STRING,
        video_publish_TS TIMESTAMP,
        loading_TS TIMESTAMP""",
    "youtube.video_statistics": """
        video_id STRING, 
        video_view_count INT, 
        video_like_count INT, 
        video_comment_count INT,
        loading_TS TIMESTAMP""", 
}

# for each key,value in the metadata object SILVER_TABLE_SCHEMAS 
for table, ddl in SILVER_TABLE_SCHEMAS.items(): 
    
    # create a dynmamic Spark SQL script, reading from the Variable Library variables, and the SILVER_TABLE_SCHEMAS metadata
    create_script = f"CREATE TABLE IF NOT EXISTS `{lh_workspace_name}`.`{silver_lh_name}`.{table} ({ddl});" 
    
    # CREATE OR REPLACE, useful for development for iterating on table schema design
    #create_script = f"CREATE OR REPLACE TABLE `{lh_workspace_name}`.`{silver_lh_name}`.{table} ({ddl});" 
    
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
