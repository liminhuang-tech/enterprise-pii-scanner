# -*- coding: utf-8 -*-
import os, sys, requests, urllib3, pandas as pd
import json
from requests.auth import HTTPBasicAuth
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StringType
from concurrent.futures import ThreadPoolExecutor
from requests_kerberos import HTTPKerberosAuth, OPTIONAL

# Disable SSL warnings for environments with self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "pii_rules.json")

with open(CONFIG_PATH, 'r') as f:
    config_data = json.load(f)

atlas_cfg = config_data.get("atlas_config", {})
scan_cfg = config_data.get("scan_config", {})

DATABASE_NAME = atlas_cfg.get("database_name", "iceberg_db")
ATLAS_URL = atlas_cfg.get("url")
ATLAS_AUTH = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
MAX_PARALLEL_TABLES = scan_cfg.get("max_parallel_tables", 5)
SAMPLE_ROWS = scan_cfg.get("sample_rows", 1000)


_ANALYZER = None

def get_analyzer_singleton():
    """
    Initialize Presidio with full profile support + Custom Parsers.
    Uses Singleton pattern to ensure Spacy models are loaded only once per Executor process.
    """
    global _ANALYZER
    if _ANALYZER is None:
        import spacy
        from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern
        from presidio_analyzer.nlp_engine import SpacyNlpEngine
        
        # Force load English small model for NLP logic
        original_load = spacy.load
        spacy.load = lambda name, **kwargs: original_load('en_core_web_sm', **kwargs)
        
        nlp_engine = SpacyNlpEngine()
        nlp_engine.nlp = {"en": original_load("en_core_web_sm")}
        
        # Default threshold allows all Presidio built-in profiles (Email, Person, Credit Card, etc.)
        engine = AnalyzerEngine(nlp_engine=nlp_engine, default_score_threshold=0.4)

        # Custom Parser for specific National IDs
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, 'r') as f:
                rules_file = json.load(f)
                custom_rules = rules_file.get("custom_patterns", [])
                for rule in custom_rules:
                    new_pattern = Pattern(
                        name=rule["name"], 
                        regex=rule["regex"], 
                        score=rule["score"]
                    )
                    
                    new_recognizer = PatternRecognizer(
                        supported_entity=rule["entity"], 
                        patterns=[new_pattern]
                    )
                    
                    engine.registry.add_recognizer(new_recognizer)
            print(f" ⚙ Loaded {len(custom_rules)} custom PII rules from config.")
        else:
            print(" ⚠ Warning: pii_rules.json not found, using default built-in rules.")
       
        _ANALYZER = engine
    return _ANALYZER

@pandas_udf(StringType())
def pii_udf(batch: pd.Series) -> pd.Series:
    """
    Vectorized UDF using Apache Arrow for high-performance scanning.
    Scales across Spark Executors to handle millions of rows.
    """
    try:
        analyzer = get_analyzer_singleton()
    except Exception as e:
        return batch.apply(lambda x: f"INIT_ERROR: {str(e)[:20]}")

    def scan(text):
        if not text or len(str(text)) < 2: 
            return "SAFE"
        try:
            # Presidio analyze returns all matching PII types
            results = analyzer.analyze(text=str(text), language="en")
            if not results: 
                return "SAFE"
            
            etype = results[0].entity_type
            # Filter out common false positives for numeric IDs
            if etype in ["DATE_TIME"]: 
                return "SAFE"
            return etype
        except:
            return "SCAN_ERROR"
            
    return batch.apply(scan)


def tag_entity(table, pii_type, column=None):
    """
    Handles Atlas Metadata tagging. 
    Includes Pre-checks and Error handling for 10k table scale.
    """
   
    is_column = column is not None
    entity_type = "iceberg_column" if is_column else "iceberg_table"
    search_query = f"{table} {column}" if is_column else table
    label = f"{table}.{column}" if is_column else table
    display_label = f"{table} -> column: {column}" if is_column else f"{table} (Table-level)"
    try:
        # Step 1: Search for the specific column entity
        search_url = f"{ATLAS_URL}/search/basic"
        params = {"query": search_query, "typeName": entity_type, "limit": 10, "attributes": "classifications,qualifiedName"}
        r = requests.get(search_url, params=params, auth=ATLAS_AUTH, verify=False, timeout=10)
       
        
        if r.status_code == 401:
            print(f" ❌ [ATLAS] Kerberos Authentication Failed. Did you run 'kinit'?")
            return
        
        if r.status_code != 200:
            print(f" ❌ [ATLAS] Search failed for {display_label}")
            return

        entities = r.json().get("entities", [])
        guid = None
        existing_tags = []

        # Precision matching using qualifiedName to handle 1000+ column environments
        if is_column:
            target_marker = f"{DATABASE_NAME}.{table}.{column}@"
        else:
            target_marker = f"{DATABASE_NAME}.{table}@"
            
        for ent in entities:
            qn = ent.get("attributes", {}).get("qualifiedName", "") or \
                 ent.get("qualifiedName") or \
                 ent.get("displayText") or ""
         
            if target_marker in qn:
                tmp_guid = ent.get("guid")
                if str(tmp_guid) == "-1":
                    lookup_url = f"{ATLAS_URL}/entity/uniqueAttribute/type/{entity_type}"
                    l_resp = requests.get(lookup_url, params={"attr:qualifiedName": qn}, auth=ATLAS_AUTH, verify=False, timeout=10)
                    if l_resp.status_code == 200:
                        real_data = l_resp.json().get("entity", {})
                        guid = real_data.get("guid")
                        existing_tags = [c.get("typeName") for c in real_data.get("classifications", [])]
                else:
                    guid = tmp_guid
                    existing_tags = [c.get("typeName") for c in ent.get("classifications", [])]
                break

        if not guid or str(guid) == "-1":
            print(f" ⚠ [ATLAS] Guid not found for {display_label}")   
            return

        # Optimization: Skip if already tagged to reduce Atlas API overhead
        if "PII" in existing_tags:
            print(f" ℹ {display_label}: Already tagged (Pre-check skip)")
            return

        # Step 2: Post Classification
        tag_url = f"{ATLAS_URL}/entity/guid/{guid}/classifications"
        resp = requests.post(tag_url, json=[{"typeName": "PII"}], auth=ATLAS_AUTH, verify=False, timeout=10)
        
        if resp.status_code in [200, 204]:
            print(f" ✅ {display_label}: Tagged as PII ({pii_type})")
        else:
            # Handle race conditions in highly parallel environments
            if "already associated" in resp.text:
                print(f" ℹ {display_label}: Already tagged (API Conflict skip)")
            else:
                print(f" ❌ {display_label}: Tag failed - {resp.text}")

    except Exception as e:
        print(f" ❌ [ATLAS ERR] {str(e)}")

def process_table_task(spark, t):
    """
    Workhorse function for a single table. 
    Handles data sampling, PII detection, and unpersisting cache.
    """
    try:
        print(f"\n📂 STARTING SCAN: {t}")
        df = spark.table(f"{DATABASE_NAME}.{t}")
        # Automatically identify string columns for PII scanning
        str_cols = [f.name for f in df.schema.fields if "string" in str(f.dataType).lower()]
        
        table_has_pii = False

        for c in str_cols:
            # Scale Optimization: Filter NULLs first, then sample to ensure high-quality data
            df_sample = df.select(c) \
                          .filter(col(c).isNotNull()) \
                          .filter(col(c) != "") \
                          .limit(SAMPLE_ROWS) \
                          .cache()

            if df_sample.count() == 0:
                print(f" ⚪ Column {c}: Skipping (All NULL or Empty)")
                df_sample.unpersist()
                continue

            # Run Distributed PII Detection
            results = df_sample.select(pii_udf(col(c)).alias("res")) \
                               .filter("res NOT IN ('SAFE', 'SCAN_ERROR')") \
                               .groupBy("res").count() \
                               .orderBy("count", ascending=False).limit(1).collect()

            if results:
                detected_type = results[0]["res"]
                print(f" 🚩 {t}.{c}: Detected {detected_type}")
                tag_entity(t, detected_type, column=c)
                table_has_pii = True
            else:
                print(f" ✅ {t}.{c}: Clean")
            
            # Critical for 10k tables: Release memory after processing each column
            df_sample.unpersist()
        if table_has_pii:
            print(f" 🎯 Table {t} has PII columns. Syncing table-level tag...")
            tag_entity(t, "PII") 
    except Exception as e:
        print(f" ⚠ Error processing table {t}: {e}")

def run():
    """
    Main Runner: Generic table discovery and Parallel Execution.
    """
    spark = SparkSession.builder \
        .appName("PII_Enterprise_Discovery_Scale") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # Generic logic to fetch all tables in the database
    tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {DATABASE_NAME}").collect()]
    print(f"🚀 Found {len(tables)} tables. Scaling out with {MAX_PARALLEL_TABLES} concurrent jobs...")

    # Requirement [Scale]: Multi-threading allows Driver to submit multiple Spark Jobs 
    # simultaneously, maximizing cluster throughput for thousands of tables.
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_TABLES) as executor:
        for t in tables:
            executor.submit(process_table_task, spark, t)

if __name__ == "__main__":
    run()
