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

# # nb_log_load_end_ko — Fin de chargement (échec)
# 
# Appelé **après l'échec du chargement d'un objet** pour enregistrer la fin dans
# `observability.log_load`. Les compteurs `processings_success`/`processings_failed` sont
# calculés dynamiquement depuis `observability.log_processing` (dédoublonné par
# `processing_name`, dernier événement par étape — un retry d'étape ne compte qu'une fois).
# 
# Le `Logger` recalcule et **impose** le statut final à partir des compteurs — le notebook ne
# transmet aucun `status` explicite ; seule la présence d'un `error_message` distingue ce
# notebook de `nb_log_load_end_ok`.
# 
# | Paramètre | Type | Obligatoire | Description |
# |---|---|---|---|
# | `sequence_id` | string | Oui | Identifiant de la séquence parente |
# | `load_id` | string | Oui | Identifiant du chargement — retourné par `nb_log_load_start` |
# | `object_name` | string | Oui | Nom de l'objet chargé |
# | `processings_count` | int | Oui | Nombre d'étapes techniques attendues pour ce chargement |
# | `error_message` | string | Non | Message d'erreur optionnel (ex : message d'exception) |


# PARAMETERS CELL ********************

# Valeurs injectées par l'activité Notebook du pipeline Fabric
sequence_id       = ""  # str — identifiant de séquence
load_id           = ""  # str — identifiant de chargement, retourné par nb_log_load_start
object_name       = ""  # str — nom de l'objet chargé
processings_count = 0   # int — nombre d'étapes techniques attendues
error_message     = ""  # str — message d'erreur optionnel

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation des paramètres obligatoires
assert sequence_id, "sequence_id est obligatoire — fourni par l'activité Notebook Fabric."
assert load_id, "load_id est obligatoire — fourni par l'activité Notebook Fabric."
assert object_name, "object_name est obligatoire — fourni par l'activité Notebook Fabric."
assert processings_count, "processings_count est obligatoire — fourni par l'activité Notebook Fabric."

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from dp_framework.config.models import ProcessingResult
from dp_framework.logging.logger import Logger

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ------------------------------------------------------------------
# Calcul dynamique des compteurs et métriques depuis log_processing
# Dernier événement par (load_id, processing_name) — un retry d'étape ne compte qu'une fois.
# ------------------------------------------------------------------

w = Window.partitionBy("load_id", "processing_name").orderBy(F.col("event_time").desc())

df_latest = (
    spark.table("lh_storage.observability.log_processing")
    .filter(F.col("load_id") == load_id)
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

processings_success = df_latest.filter(F.col("status") == "success").count()
processings_failed  = df_latest.filter(F.col("status") == "failure").count()

# Bilan des lignes traitées, agrégé sur toutes les étapes techniques du chargement.
metrics = df_latest.agg(
    F.sum("rows_read").alias("rows_read"),
    F.sum("rows_valid").alias("rows_valid"),
    F.sum("rows_rejected").alias("rows_rejected"),
    F.sum("rows_inserted").alias("rows_inserted"),
    F.sum("rows_updated").alias("rows_updated"),
    F.sum("rows_deleted").alias("rows_deleted"),
).first()

print(f"load_id             : {load_id}")
print(f"processings_count   : {processings_count}")
print(f"processings_success : {processings_success}")
print(f"processings_failed  : {processings_failed}")
print(f"error_message       : {error_message}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ------------------------------------------------------------------
# Enregistrement de la fin de chargement
# ------------------------------------------------------------------

_logger = Logger(spark, "lh_storage")
_logger.log_load_end(
    sequence_id=sequence_id,
    load_id=load_id,
    object_name=object_name,
    processings_count=int(processings_count),
    processings_success=processings_success,
    processings_failed=processings_failed,
    error_message=error_message or None,
    result=ProcessingResult(
        table_name=object_name,
        status="failure",
        rows_read=metrics["rows_read"] or 0,
        rows_valid=metrics["rows_valid"] or 0,
        rows_rejected=metrics["rows_rejected"] or 0,
        rows_inserted=metrics["rows_inserted"] or 0,
        rows_updated=metrics["rows_updated"] or 0,
        rows_deleted=metrics["rows_deleted"] or 0,
    ),
)

notebookutils.notebook.exit("KO")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
