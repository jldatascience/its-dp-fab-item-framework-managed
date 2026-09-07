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

# # nb_log_load_start — Démarrage du chargement d'un objet
# 
# Appelé **avant le chargement complet d'un objet** (une passe INGESTION + étapes techniques
# associées) pour enregistrer le démarrage dans `observability.log_load`. Retourne un `load_id`
# unique (UUID4) via `notebookutils.notebook.exit()` pour injection dans les notebooks
# `nb_log_processing_start`, `nb_log_load_end_ok` / `nb_log_load_end_ko`.
# 
# | Paramètre | Type | Obligatoire | Description |
# |---|---|---|---|
# | `sequence_id` | string | Oui | Identifiant de la séquence parente — retourné par `nb_log_sequence_start` |
# | `object_name` | string | Oui | Nom de l'objet à charger (ex : `groupementgup`) |
# | `processings_count` | int | Oui | Nombre d'étapes techniques attendues pour ce chargement |

# PARAMETERS CELL ********************

# Valeurs injectées par l'activité Notebook du pipeline Fabric
sequence_id       = ""  # str — identifiant de séquence, retourné par nb_log_sequence_start
object_name       = ""  # str — nom de l'objet à charger (ex : groupementgup)
processings_count = 0   # int — nombre d'étapes techniques attendues

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation des paramètres obligatoires
assert sequence_id, "sequence_id est obligatoire — fourni par l'activité Notebook Fabric."
assert object_name, "object_name est obligatoire — fourni par l'activité Notebook Fabric."
assert processings_count, "processings_count est obligatoire — fourni par l'activité Notebook Fabric."

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
# Démarrage du chargement — génère et retourne le load_id
# ------------------------------------------------------------------

_logger = Logger(spark, "lh_storage")
load_id = _logger.log_load_start(sequence_id, object_name, int(processings_count))

print(f"sequence_id       : {sequence_id}")
print(f"object_name       : {object_name}")
print(f"processings_count : {processings_count}")
print(f"load_id           : {load_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retourne le load_id au pipeline Fabric pour injection dans les activités suivantes
notebookutils.notebook.exit(load_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
