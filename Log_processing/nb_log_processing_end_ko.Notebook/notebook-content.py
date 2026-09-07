# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "2ddf2f7f-be1a-9ba7-488a-75aff53266f3",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_log_processing_end_ko — Fin de traitement (échec)
# 
# Appelé **après l'échec d'une étape technique** pour enregistrer la fin en erreur dans `observability.log_processing`.
# Doit être appelé avec le `processing_id` retourné par `nb_log_processing_start`.
# 
# | Paramètre | Type | Obligatoire | Description |
# |---|---|---|---|
# | `sequence_id` | string | Oui | Identifiant de la séquence parente |
# | `load_id` | string | Oui | Identifiant du chargement parent |
# | `processing_id` | string | Oui | Identifiant du traitement — retourné par `nb_log_processing_start` |
# | `object_name` | string | Oui | Nom de l'objet traité |
# | `processing_name` | string | Oui | Nom de l'étape technique (ex : `INGESTION`, `SCD2`, `SOFT_DELETE`) |
# | `error_message` | string | Non | Message d'erreur décrivant la cause de l'échec (défaut : vide) |
# | `rows_read` | int | Non | Nombre de lignes lues avant l'échec (défaut : 0) |
# | `rows_valid` | int | Non | Nombre de lignes valides avant l'échec (défaut : 0) |
# | `rows_rejected` | int | Non | Nombre de lignes rejetées avant l'échec (défaut : 0) |
# | `rows_inserted` | int | Non | Nombre de lignes insérées avant l'échec (défaut : 0) |
# | `rows_updated` | int | Non | Nombre de lignes mises à jour avant l'échec (défaut : 0) |
# | `rows_deleted` | int | Non | Nombre de lignes supprimées avant l'échec (défaut : 0) |


# PARAMETERS CELL ********************

# Valeurs injectées par l'activité Notebook du pipeline Fabric
sequence_id     = ""  # str — identifiant de séquence
load_id         = ""  # str — identifiant de chargement
processing_id   = ""  # str — identifiant du traitement, retourné par nb_log_processing_start
object_name     = ""  # str — nom de l'objet traité
processing_name = ""  # str — nom de l'étape technique (ex : INGESTION)
error_message   = ""  # str — message d'erreur décrivant la cause de l'échec

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialisation des métriques
rows_read       = 0   # int — nombre de lignes lues avant l'échec
rows_valid      = 0   # int — nombre de lignes valides avant l'échec
rows_rejected   = 0   # int — nombre de lignes rejetées avant l'échec
rows_inserted   = 0   # int — nombre de lignes insérées avant l'échec
rows_updated    = 0   # int — nombre de lignes mises à jour avant l'échec
rows_deleted    = 0   # int — nombre de lignes supprimées avant l'échec

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation des paramètres obligatoires
assert sequence_id,     "sequence_id est obligatoire — fourni par l'activité Notebook Fabric."
assert load_id,         "load_id est obligatoire — fourni par l'activité Notebook Fabric."
assert processing_id,   "processing_id est obligatoire — retourné par nb_log_processing_start."
assert object_name,     "object_name est obligatoire — fourni par l'activité Notebook Fabric."
assert processing_name, "processing_name est obligatoire — fourni par l'activité Notebook Fabric."

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging

from dp_framework.logging.logger import Logger
from dp_framework.config.models import ProcessingResult

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ------------------------------------------------------------------
# Enregistrement de la fin de traitement (échec)
# ------------------------------------------------------------------

result = ProcessingResult(
    table_name=object_name,
    status="failure",
    rows_read=int(rows_read),
    rows_valid=int(rows_valid),
    rows_rejected=int(rows_rejected),
    rows_inserted=int(rows_inserted),
    rows_updated=int(rows_updated),
    rows_deleted=int(rows_deleted),
    error_message=error_message or None,
)

_logger = Logger(spark, "lkh_storage")
_logger.log_processing_end(sequence_id, load_id, processing_id, object_name, processing_name, result)

print(f"object_name     : {object_name}")
print(f"processing_name : {processing_name}")
print(f"status          : {result.status}")
print(f"error_message   : {result.error_message}")
print(f"rows_read       : {result.rows_read}")
print(f"rows_valid      : {result.rows_valid}")
print(f"rows_rejected   : {result.rows_rejected}")
print(f"rows_inserted   : {result.rows_inserted}")
print(f"rows_updated    : {result.rows_updated}")
print(f"rows_deleted    : {result.rows_deleted}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.exit("KO")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
