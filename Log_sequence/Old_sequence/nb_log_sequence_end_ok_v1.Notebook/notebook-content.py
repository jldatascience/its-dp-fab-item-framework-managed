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
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "151b9641-d953-aee6-43a2-98c011e656a5",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_log_sequence_end_ok — Fin de séquence (succès)
# 
# Appelé en **fin de pipeline** (branche succès) pour enregistrer la fin avec succès d'une séquence.
# Les compteurs `tables_count`, `tables_success`, `tables_failed` sont calculés dynamiquement
# depuis `observability.log_processing` pour le `batch_id` courant.
# 
# | Paramètre | Type | Obligatoire | Description |
# |---|---|---|---|
# | `batch_id` | string | Oui | Identifiant de la séquence — retourné par `nb_log_sequence_start` |

# PARAMETERS CELL ********************

# Valeurs injectées par l'activité Notebook du pipeline Fabric
batch_id = ""  # str — identifiant de séquence, retourné par nb_log_sequence_start
sequence_name =""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation des paramètres obligatoires
assert batch_id, "batch_id est obligatoire — fourni par l'activité Notebook Fabric."
assert sequence_name, "sequence_name est obligatoire — fourni par l'activité Notebook Fabric."


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from dp_framework.logging.logger import Logger

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ------------------------------------------------------------------
# Calcul dynamique des compteurs depuis log_processing
# Dernier événement par processing_id pour ce batch_id.
# ------------------------------------------------------------------

w = Window.partitionBy("processing_id").orderBy(F.col("event_time").desc())

df_latest = (
    spark.table("lh_storage.observability.log_processing")
    .filter(F.col("batch_id") == batch_id)
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

tables_count   = df_latest.count()
tables_success = df_latest.filter(F.col("status") == "success").count()
tables_failed  = tables_count - tables_success

print(f"batch_id       : {batch_id}")
print(f"tables_count   : {tables_count}")
print(f"tables_success : {tables_success}")
print(f"tables_failed  : {tables_failed}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ------------------------------------------------------------------
# Enregistrement de la fin de séquence (succès)
# ------------------------------------------------------------------

_logger = Logger(spark, "lh_storage")

_logger.end_sequence(
    batch_id=batch_id,
    sequence_name=sequence_name,
    status="success",
    tables_count=tables_count,
    tables_success=tables_success,
    tables_failed=tables_failed,
)

notebookutils.notebook.exit("OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
