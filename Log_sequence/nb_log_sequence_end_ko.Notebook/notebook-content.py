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
# META       "environmentId": "2ddf2f7f-be1a-9ba7-488a-75aff53266f3",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_log_sequence_end_ko — Fin de séquence (échec)
# 
# Appelé en **fin de pipeline** (branche échec) pour enregistrer la fin en erreur d'une séquence.  
# Les compteurs `objects_count`, `objects_success`, `objects_failed` sont calculés dynamiquement  
# depuis `observability.log_load` (dédoublonné par `load_id`) pour le `sequence_id` courant.
# 
# | Paramètre       | Type   | Obligatoire | Description                                                                 |
# |-----------------|--------|------------|-----------------------------------------------------------------------------|
# | `sequence_id`   | string | Oui        | Identifiant de la séquence — retourné par `nb_log_sequence_start`          |
# | `sequence_name` | string | Oui        | Nom fonctionnel de la séquence — fourni par le pipeline appelant     |
# | `error_message` | string | Non        | Message d'erreur optionnel (ex : message d'exception du pipeline)          |


# PARAMETERS CELL ********************

# Valeurs injectées par l'activité Notebook du pipeline Fabric
sequence_id   = ""  # str — identifiant de séquence, retourné par nb_log_sequence_start
sequence_name = ""
error_message = ""  # str — message d'erreur optionnel


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation des paramètres obligatoires
assert sequence_id, "sequence_id est obligatoire — fourni par l'activité Notebook Fabric."
assert sequence_name,"sequence_name est obligatoire — fourni par l'activité Notebook Fabric."

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
# Calcul dynamique des compteurs depuis log_load
# Dernier événement par load_id pour cette sequence_id.
# ------------------------------------------------------------------

w = Window.partitionBy("load_id").orderBy(F.col("event_time").desc())

df_latest = (
    spark.table("lh_storage.observability.log_load")
    .filter(F.col("sequence_id") == sequence_id)
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

objects_count   = df_latest.count()
objects_success = df_latest.filter(F.col("status") == "success").count()
objects_failed  = objects_count - objects_success

print(f"sequence_id     : {sequence_id}")
print(f"objects_count   : {objects_count}")
print(f"objects_success : {objects_success}")
print(f"objects_failed  : {objects_failed}")
print(f"error_message   : {error_message}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ------------------------------------------------------------------
# Enregistrement de la fin de séquence (échec)
# ------------------------------------------------------------------

_logger = Logger(spark, "lh_storage")
_logger.end_sequence(
    sequence_id=sequence_id,
    sequence_name="",
    status="failure",
    objects_count=objects_count,
    objects_success=objects_success,
    objects_failed=objects_failed,
    error_message=error_message or None,
)

notebookutils.notebook.exit("KO")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
