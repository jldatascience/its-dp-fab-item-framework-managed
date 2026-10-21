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
# META           "id": "0d73cf55-a598-4d6e-9632-806fd452cb19"
# META         },
# META         {
# META           "id": "22885ae1-ec38-4913-bd38-c73c8dc628e3"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Génération des vues DSO
# Ce notebook génère les scripts SQL des vues courante (`ccc`) et historisée (`hcc`) pour un objet Storage. Les scripts sont produits sans être exécutés.

# CELL ********************

from pathlib import Path
import re

object_name = "mappinggrade"
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
# Cette cellule lit les colonnes métier de l'objet Storage et crée les deux fichiers DDL.

# CELL ********************

source_table = f"{storage_lakehouse}.{storage_schema}.{object_name}"
columns = spark.table(source_table).columns
business_columns = [column for column in columns if not column.startswith("_")]

if "_is_current" not in columns:
    raise ValueError(f"Colonne _is_current absente de la table : {source_table}")
if not business_columns:
    raise ValueError(f"Aucune colonne métier à exposer pour : {source_table}")

projection = ",\n    ".join(f"[{column}]" for column in business_columns)
current_view_name = f"{object_name}_ccc_v{major_version}"
historized_view_name = f"{object_name}_hcc_v{major_version}"

current_sql = f"""CREATE OR ALTER VIEW [{dso_schema}].[{current_view_name}]
AS
SELECT
    {projection}
FROM {source_table}
WHERE [_is_current] = 1;
"""
historized_sql = f"""CREATE OR ALTER VIEW [{dso_schema}].[{historized_view_name}]
AS
SELECT
    {projection}
FROM {source_table};
"""

destination = Path(output_dir)
destination.mkdir(parents=True, exist_ok=True)
current_path = destination / f"{current_view_name}.sql"
historized_path = destination / f"{historized_view_name}.sql"
current_path.write_text(current_sql, encoding="utf-8")
historized_path.write_text(historized_sql, encoding="utf-8")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Affichage des résultats
# Les scripts affichés ci-dessous sont à exécuter depuis le contexte SQL du Lakehouse DSO après revue.

# CELL ********************

print(f"Scripts générés :\n- {current_path}\n- {historized_path}\n")
print("Vue courante :")
print(current_sql)
print("Vue historisée :")
print(historized_sql)

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
