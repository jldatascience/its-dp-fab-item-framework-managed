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

# # nb_log_processing_start — Démarrage d'une étape de traitement
# 
# Appelé **avant chaque étape technique du chargement d'un objet** pour enregistrer le démarrage
# dans `observability.log_processing`. Retourne un `processing_id` unique (UUID4) via
# `notebookutils.notebook.exit()` pour injection dans les notebooks
# `nb_log_processing_end_ok` / `nb_log_processing_end_ko`.
# 
# | Paramètre | Type | Obligatoire | Description |
# |---|---|---|---|
# | `sequence_id` | string | Oui | Identifiant de la séquence parente — retourné par `nb_log_sequence_start` |
# | `load_id` | string | Oui | Identifiant du chargement parent — retourné par `nb_log_load_start` |
# | `object_name` | string | Oui | Nom de l'objet en cours de traitement (ex : `groupementgup`) |
# | `processing_name` | string | Oui | Nom de l'étape technique (ex : `INGESTION`, `SCD2`, `SOFT_DELETE`) |

# PARAMETERS CELL ********************

# Valeurs injectées par l'activité Notebook du pipeline Fabric
sequence_id     = ""  # str — identifiant de séquence, retourné par nb_log_sequence_start
load_id         = ""  # str — identifiant de chargement, retourné par nb_log_load_start
object_name     = ""  # str — nom de l'objet à traiter (ex : groupementgup)
processing_name = ""  # str — nom de l'étape technique (ex : INGESTION)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation des paramètres obligatoires
assert sequence_id,     "sequence_id est obligatoire — fourni par l'activité Notebook Fabric."
assert load_id,         "load_id est obligatoire — fourni par l'activité Notebook Fabric."
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

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ------------------------------------------------------------------
# Démarrage du traitement — génère et retourne le processing_id
# ------------------------------------------------------------------

_logger = Logger(spark, "lkh_storage")
processing_id = _logger.log_processing_start(sequence_id, load_id, object_name, processing_name)

print(f"sequence_id     : {sequence_id}")
print(f"load_id         : {load_id}")
print(f"object_name     : {object_name}")
print(f"processing_name : {processing_name}")
print(f"processing_id   : {processing_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retourne le processing_id au pipeline Fabric pour injection dans nb_log_processing_end_*
notebookutils.notebook.exit(processing_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
