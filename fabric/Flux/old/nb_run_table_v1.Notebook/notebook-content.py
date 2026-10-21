# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "22885ae1-ec38-4913-bd38-c73c8dc628e3",
# META       "default_lakehouse_name": "lh_storage",
# META       "default_lakehouse_workspace_id": "a699158a-f604-431c-b9f4-aff776a02c5d",
# META       "known_lakehouses": [
# META         {
# META           "id": "22885ae1-ec38-4913-bd38-c73c8dc628e3"
# META         },
# META         {
# META           "id": "ed697d62-5f1d-4db4-9a35-1b55ab1d12cc"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "2ddf2f7f-be1a-9ba7-488a-75aff53266f3",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# PARAMETERS
table_name          = ""            # injecté par Data Factory (ForEach)
batch_id              = ""            # = batch_id de la séquence (nb_log_sequence_start)
lakehouse           = ""
metadata_base_path  = ""   # ou .../bdp selon le data product

print(f"[PARAMS] table_name={table_name} | batch_id={batch_id} | lakehouse={lakehouse} | metadata_base_path={metadata_base_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from dp_framework import Pipeline
from dp_framework.config.yaml_reader import YamlReader

print("[1/4] Lecture de la configuration YAML...")

reader = YamlReader(spark, metadata_base_path)

mapping = reader.read_mapping(table_name)
print(f"  ✔ mapping chargé : dp_type={mapping.dp_type}, source={mapping.source_schema}.{mapping.source_table}, "
      f"target={mapping.target_schema}.{mapping.target_table}, scd_type={mapping.scd_type}, "
      f"load_mode={mapping.load_mode}, soft_delete={mapping.soft_delete}")

connections = reader.read_connections()
print(f"  ✔ connexions chargées : {list(connections.keys())}")

contract = None
if mapping.dp_type == "ADP":
    contract = reader.read_contract(table_name)
    print(f"  ✔ contract chargé : {len(contract.columns)} colonne(s)")
    display(spark.createDataFrame([(c.name, c.source_name, c.type, c.functional_key) for c in contract.columns],
                                   ["name", "source_name", "type", "functional_key"]))
else:
    print("  ℹ️ BDP — pas de contract à charger")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from dp_framework.processors.reader import SourceReader

preview_reader = SourceReader(spark, lakehouse, connections)
df_source = preview_reader.read(mapping, last_watermark=None)

print(f"[SOURCE] {mapping.source_schema}.{mapping.source_table} — {df_source.count()} ligne(s)")
df_source.printSchema()
display(df_source)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL ********************

print("[2/4] Initialisation du Pipeline...")

pipeline = Pipeline(
    spark,
    lakehouse=lakehouse,
    dp_type=mapping.dp_type,
    connections=connections,
)
print(f"  ✔ Pipeline prêt — dp_type={mapping.dp_type}, lakehouse={lakehouse}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table = f"{lakehouse}.{mapping.target_schema}.{mapping.target_table}"
table_existed_before = spark.catalog.tableExists(target_table)
print(f"Table cible '{target_table}' existante avant ce run : {table_existed_before}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



print(f"[3/4] Exécution du pipeline pour la table '{table_name}' (batch_id={batch_id})...")

result = pipeline.run(mapping, batch_id=batch_id, contract=contract)

print(f"  ✔ Pipeline terminé — statut={result.status}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


print("[4/4] Résultat détaillé :")

display(spark.createDataFrame([{
    "table_name": result.table_name,
    "status": result.status,
    "rows_read": result.rows_read,
    "rows_valid": result.rows_valid,
    "rows_rejected": result.rows_rejected,
    "rows_inserted": result.rows_inserted,
    "rows_updated": result.rows_updated,
    "rows_deleted": result.rows_deleted,
    "error_message": result.error_message or "",   # évite le None
}]))

if result.status == "failure":
    print(f"  ❌ ÉCHEC : {result.error_message}")
    raise Exception(f"Pipeline failed for {result.table_name}: {result.error_message}")

print("  ✅ SUCCÈS")

import json
notebookutils.notebook.exit(json.dumps({
    "status": result.status,
    "table_name": result.table_name,
    "rows_read": result.rows_read,
    "rows_inserted": result.rows_inserted,
    "rows_updated": result.rows_updated,
    "rows_deleted": result.rows_deleted,
}))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
