# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "151b9641-d953-aee6-43a2-98c011e656a5",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_log_processing_start — Démarrage du traitement d'une table
# 
# Appelé **avant le chargement d'une table** pour enregistrer le démarrage dans `observability.log_processing`.
# Retourne un `processing_id` unique (UUID4) via `notebookutils.notebook.exit()`
# pour injection dans les notebooks `nb_log_processing_end_ok` / `nb_log_processing_end_ko`.
# 
# | Paramètre | Type | Obligatoire | Description |
# |---|---|---|---|
# | `batch_id` | string | Oui | Identifiant de la séquence parente — retourné par `nb_log_sequence_start` |
# | `table_name` | string | Oui | Nom de la table en cours de traitement (ex : `groupementgup`) |

# PARAMETERS CELL ********************

# Valeurs injectées par l'activité Notebook du pipeline Fabric
batch_id   = ""  # str — identifiant de séquence, retourné par nb_log_sequence_start
table_name = ""  # str — nom de la table à traiter (ex : groupementgup)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"batch_id = [{batch_id}]")
print(f"table_name = [{table_name}]")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation des paramètres obligatoires
assert batch_id,   "batch_id est obligatoire — fourni par l'activité Notebook Fabric."
assert table_name, "table_name est obligatoire — fourni par l'activité Notebook Fabric."

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
processing_id = _logger.log_processing_start(batch_id, table_name)

print(f"batch_id      : {batch_id}")
print(f"table_name    : {table_name}")
print(f"processing_id : {processing_id}")

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

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
