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

# # nb_log_processing_end_ok — Fin de traitement (succès)
# 
# Appelé **après le chargement réussi d'une table** pour enregistrer la fin avec succès dans `observability.log_processing`.
# Doit être appelé avec le `processing_id` retourné par `nb_log_processing_start`.
# 
# | Paramètre | Type | Obligatoire | Description |
# |---|---|---|---|
# | `batch_id` | string | Oui | Identifiant de la séquence parente |
# | `processing_id` | string | Oui | Identifiant du traitement — retourné par `nb_log_processing_start` |
# | `table_name` | string | Oui | Nom de la table traitée |
# | `rows_read` | int | Non | Nombre de lignes lues en source (défaut : 0) |
# | `rows_valid` | int | Non | Nombre de lignes valides après contrat (défaut : 0) |
# | `rows_rejected` | int | Non | Nombre de lignes rejetées (défaut : 0) |
# | `rows_inserted` | int | Non | Nombre de lignes insérées en cible (défaut : 0) |
# | `rows_updated` | int | Non | Nombre de lignes mises à jour en cible (défaut : 0) |
# | `rows_deleted` | int | Non | Nombre de lignes supprimées (soft delete) en cible (défaut : 0) |


# PARAMETERS CELL ********************

# Valeurs injectées par l'activité Notebook du pipeline Fabric
batch_id      = ""  # str — identifiant de séquence
processing_id = ""  # str — identifiant du traitement, retourné par nb_log_processing_start
table_name    = ""  # str — nom de la table traitée
rows_read     = 0   # int — nombre de lignes lues en source
rows_valid    = 0   # int — nombre de lignes valides après contrat
rows_rejected = 0   # int — nombre de lignes rejetées
rows_inserted = 0   # int — nombre de lignes insérées en cible
rows_updated  = 0   # int — nombre de lignes mises à jour en cible
rows_deleted  = 0   # int — nombre de lignes supprimées (soft delete)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation des paramètres obligatoires
assert batch_id,      "batch_id est obligatoire — fourni par l'activité Notebook Fabric."
assert processing_id, "processing_id est obligatoire — retourné par nb_log_processing_start."
assert table_name,    "table_name est obligatoire — fourni par l'activité Notebook Fabric."

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
# Enregistrement de la fin de traitement (succès)
# ------------------------------------------------------------------

result = ProcessingResult(
    table_name=table_name,
    status="success",
    rows_read=int(rows_read),
    rows_valid=int(rows_valid),
    rows_rejected=int(rows_rejected),
    rows_inserted=int(rows_inserted),
    rows_updated=int(rows_updated),
    rows_deleted=int(rows_deleted),
)

_logger = Logger(spark, "lkh_storage")
_logger.log_processing_end(batch_id, processing_id, result)

print(f"table_name    : {result.table_name}")
print(f"status        : {result.status}")
print(f"rows_read     : {result.rows_read}")
print(f"rows_valid    : {result.rows_valid}")
print(f"rows_rejected : {result.rows_rejected}")
print(f"rows_inserted : {result.rows_inserted}")
print(f"rows_updated  : {result.rows_updated}")
print(f"rows_deleted  : {result.rows_deleted}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.exit("OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
