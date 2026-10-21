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
# META           "id": "0d73cf55-a598-4d6e-9632-806fd452cb19"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Génération des vues DSO
# Ce notebook génère les scripts SQL des vues courante (`ccc`) et historisée (`hcc`) pour toutes les tables d'un schéma Storage. Les scripts sont affichés et enregistrés dans `Files/dso-sql`, sans être exécutés.

# CELL ********************

from pathlib import Path

dso_schema = "MDM"
storage_lakehouse = "lh_storage"
storage_schema = "storage"
major_version = 1
output_dir = "Files/dso-sql"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Génération des scripts SQL
# Cette cellule parcourt les tables du schéma Storage, puis crée les deux fichiers DDL pour chaque table historisée.

# CELL ********************

tables = spark.catalog.listTables(storage_schema)
destination = Path(output_dir)
destination.mkdir(parents=True, exist_ok=True)
generated_scripts = []

schema_sql = f"CREATE SCHEMA IF NOT EXISTS [{dso_schema}];"
schema_path = destination / f"create_{dso_schema}_schema.sql"
schema_path.write_text(schema_sql, encoding="utf-8")
generated_scripts = [schema_sql]

for table in tables:
    if table.isTemporary or table.tableType == "VIEW":
        continue

    object_name = table.name
    source_table = f"{storage_lakehouse}.{storage_schema}.{object_name}"
    sql_source_table = f"[{storage_lakehouse}].[{storage_schema}].[{object_name}]"
    columns = spark.table(source_table).columns
    business_columns = [column for column in columns if not column.startswith("_")]

    if "_is_current" not in columns or not business_columns:
        print(f"Table ignorée : {source_table} (non historisée ou sans colonne métier)")
        continue

    projection = ",\n    ".join(f"[{column}]" for column in business_columns)
    current_view_name = f"{object_name}_ccc_v{major_version}"
    historized_view_name = f"{object_name}_hcc_v{major_version}"

    current_sql = f"""CREATE OR ALTER VIEW [{dso_schema}].[{current_view_name}]
AS
SELECT
    {projection}
FROM {sql_source_table}
WHERE [_is_current] = 1;
"""
    historized_sql = f"""CREATE OR ALTER VIEW [{dso_schema}].[{historized_view_name}]
AS
SELECT
    {projection}
FROM {sql_source_table};
"""

    current_path = destination / f"{current_view_name}.sql"
    historized_path = destination / f"{historized_view_name}.sql"
    current_path.write_text(current_sql, encoding="utf-8")
    historized_path.write_text(historized_sql, encoding="utf-8")
    generated_scripts.extend([current_sql, historized_sql])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Résultat
# Les scripts SQL ci-dessous ont également été enregistrés dans `Files/dso-sql`.

# CELL ********************

print(f"{len(generated_scripts)} scripts générés dans {destination}\n")
print("\n\n".join(generated_scripts))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_storage.storage.mappinggrade LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
