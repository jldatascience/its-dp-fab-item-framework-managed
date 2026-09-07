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

# # nb_log_sequence_start — Démarrage d'une séquence
# 
# Appelé en **début de pipeline** pour enregistrer le démarrage d'une séquence dans `observability.log_sequence`.
# Génère un `batch_id` unique (UUID4) et le retourne via `notebookutils.notebook.exit()`
# pour injection dans les activités Notebook suivantes du même pipeline.
# 
# | Paramètre | Type | Obligatoire | Description |
# |---|---|---|---|
# | `sequence_name` | string | Oui | Nom lisible de la séquence ou du pipeline (ex : `pipeline_chargement_adp`) |

# PARAMETERS CELL ********************

# Valeurs injectées par l'activité Notebook du pipeline Fabric
sequence_name = ""  # str — nom de la séquence (ex : pipeline_chargement_adp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation des paramètres obligatoires
assert sequence_name, "sequence_name est obligatoire — fourni par l'activité Notebook Fabric."

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid
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
# Génération du batch_id et démarrage de la séquence
# ------------------------------------------------------------------

batch_id = str(uuid.uuid4())

_logger = Logger(spark, "lkh_storage")
_logger.start_sequence(batch_id, sequence_name)

print(f"sequence_name : {sequence_name}")
print(f"batch_id      : {batch_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retourne le batch_id au pipeline Fabric pour injection dans les activités suivantes
notebookutils.notebook.exit(batch_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
