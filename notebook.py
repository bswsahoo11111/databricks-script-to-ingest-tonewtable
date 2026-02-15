from pyspark.sql import SparkSession
import logging

# Simple logger (remove LoggerProvider if causing issues)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("view_creator")

def run(
    spark: SparkSession,
    catalog_name: str = "dataeng",
    schema_name: str = "dataeng",
    source_table_name: str = "schema_evo_merge_schema_t3",
    target_view_name: str = "schema_evo_merge_schema_demo9",
    operation: str = "create",
    dryrun: bool = False
):
    """Create view from source table."""
    print(f"📋 Creating view: {catalog_name}.{schema_name}.{target_view_name}")
    
    create_view(
        spark=spark,
        catalog_name=catalog_name,
        schema_name=schema_name,
        source_table_name=source_table_name,
        target_view_name=target_view_name,
        operation=operation,
        dryrun=dryrun
    )

def create_view(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    source_table_name: str,
    target_view_name: str,
    operation: str = "",
    dryrun: bool = False
):
    """Create VIEW in Unity Catalog - CORRECT SYNTAX."""
    
    # 🔥 FIXED: VIEW syntax (not TABLE)
    if operation == "create":
        operation_query = "CREATE OR REPLACE VIEW"
    elif operation == "alter":
        operation_query = "ALTER VIEW"
    else:
        operation_query = "CREATE OR REPLACE VIEW"
    
    # 🔥 Set Unity Catalog context FIRST
    spark.sql(f"USE CATALOG {catalog_name}")
    spark.sql(f"USE SCHEMA {schema_name}")
    
    source_full = f"{catalog_name}.{schema_name}.{source_table_name}"
    target_full = f"{catalog_name}.{schema_name}.{target_view_name}"
    
    # 🔥 CORRECT VIEW SYNTAX - NO column definitions needed
    query_str = f"""
{operation_query} {target_full}
AS
SELECT * FROM {source_full}
    """
    
    #print(f"🔍 SQL: {query_str}")
    
    if dryrun:
        print("🔍 DRY RUN - Source preview:")
    else:
        print("🚀 EXECUTING VIEW CREATION...")
        spark.sql(query_str)
        
        # 🔥 VERIFY creation
        result = spark.sql(f"SHOW VIEWS LIKE '{target_view_name}'").collect()
        if result:
            print(f"✅ VIEW CREATED SUCCESSFULLY: {target_full}")
            #spark.sql(f"DESCRIBE {target_full}").show(5)
        else:
            print("❌ View not found - check Catalog Explorer refresh")

# Usage - Use Databricks built-in spark
run(spark, dryrun=False)


#### new branch testing

