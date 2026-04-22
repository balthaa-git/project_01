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

gold_lh_name = variables.GOLD_LH_NAME

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

GOLD_LH_SCHEMAS_TO_CREATE = ["marketing"] 

for schema_name in GOLD_LH_SCHEMAS_TO_CREATE: 
    
    # create a dynmamic Spark SQL script, reading from the Variable Library variables, and the GOLD_LH_SCHEMAS_TO_CREATE metadata
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{lh_workspace_name}`.`{gold_lh_name}`.`{schema_name}`")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# table names from the requirements diagram
# table schemas from SILVER tables
GOLD_TABLE_SCHEMAS = {
    "marketing.channels": """
        channel_surrogate_id INT, 
        channel_platform STRING,
        channel_account_name STRING,
        channel_account_description STRING,
        channel_total_subscribers INT,
        channel_total_assets INT,
        channel_total_views INT,
        modified_TS TIMESTAMP
        """,
    "marketing.assets": """
        asset_surrogate_id INT,
        asset_natural_id STRING,
        channel_surrogate_id INT,
        asset_title STRING,
        asset_text STRING, 
        asset_publish_date TIMESTAMP,
        modified_TS TIMESTAMP
        """, 
    "marketing.asset_stats": """
        asset_surrogate_id INT, 
        asset_total_impressions INT,
        asset_total_views INT, 
        asset_total_likes INT,
        asset_total_comments INT,
        modified_TS TIMESTAMP
        """, 
}

# for each key,value in the metadata object GOLD_TABLE_SCHEMAS 
for table, ddl in GOLD_TABLE_SCHEMAS.items(): 
    
    # create a dynmamic Spark SQL script, reading from the Variable Library variables, and the GOLD_TABLE_SCHEMAS metadata
    #create_script = f"CREATE TABLE IF NOT EXISTS `{lh_workspace_name}`.`{gold_lh_name}`.{table} ({ddl});" 
    
    # CREATE OR REPLACE, useful for development for iterating on table schema design
    create_script = f"CREATE OR REPLACE TABLE `{lh_workspace_name}`.`{gold_lh_name}`.{table} ({ddl});" 

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
