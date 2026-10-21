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

# # nb_log_sequence_start — Démarrage d'une séquence
# 
# Appelé en **début de pipeline** pour enregistrer le démarrage d'une séquence dans `observability.log_sequence`.
# Génère un `sequence_id` unique (UUID4) si non fourni, et le retourne via `notebookutils.notebook.exit()`
# pour injection dans les activités Notebook suivantes du même pipeline.
# 
# | Paramètre | Type | Obligatoire | Description |
# |---|---|---|---|
# | `sequence_name` | string | Oui | Nom lisible de la séquence ou du pipeline (ex : `pipeline_chargement_adp`) |
# | `sequence_id` | string | Non | Identifiant de séquence à réutiliser (ex : `@pipeline().RunId` Fabric) — généré (UUID4) si vide |

# PARAMETERS CELL ********************

# Valeurs injectées par l'activité Notebook du pipeline Fabric
sequence_name = ""  # str — nom de la séquence (ex : pipeline_chargement_adp)
sequence_id   = ""  # str — optionnel, réutilise l'id fourni (ex : @pipeline().RunId), sinon généré

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
# Génération du sequence_id (si non fourni) et démarrage de la séquence
# ------------------------------------------------------------------

sequence_id = sequence_id or str(uuid.uuid4())

_logger = Logger(spark, "lh_storage")
_logger.start_sequence(sequence_id, sequence_name)

print(f"sequence_name : {sequence_name}")
print(f"sequence_id   : {sequence_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retourne le sequence_id au pipeline Fabric pour injection dans les activités suivantes
notebookutils.notebook.exit(sequence_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
