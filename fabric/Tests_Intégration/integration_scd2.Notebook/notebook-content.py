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
# META         },
# META         {
# META           "id": "ed697d62-5f1d-4db4-9a35-1b55ab1d12cc"
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

# # Test d’intégration SCD2 – scénario complet
# 
# Ce notebook valide le fonctionnement du processeur SCD2 (`SCD2Processor`) sur une table de test dédiée, afin de vérifier :
# 
# - la création initiale des versions courantes,
# - l’idempotence (rejeu d’un même batch),
# - la gestion des changements (fermeture + nouvelle version),
# - l’absence d’anomalies sur les versions courantes,
# - le mécanisme de retry en cas d’exception `DeltaConcurrentModificationException`.
# 
# Les cellules suivantes rejouent un scénario pas à pas sur la table de test `core.scd2_atomic_test` du lakehouse `lkh_storage`.

# MARKDOWN ********************

# ### 1. Paramètres généraux du test
# 
# Cette cellule définit les paramètres utilisés pour la table de test SCD2 :
# 
# - `LAKEHOUSE` : nom du lakehouse de stockage,
# - `TARGET_SCHEMA` / `TARGET_TABLE` : schéma et table cibles,
# - `FULL_TABLE` : nom complet (3-part) de la table.
# 
# Ces variables sont réutilisées dans tout le notebook pour s’assurer que le test n’impacte que cette table dédiée.

# PARAMETERS CELL ********************

# Paramètres du test d'intégration — schéma/table dédiés pour ne pas impacter les tables réelles.
LAKEHOUSE = "lkh_storage"
TARGET_SCHEMA = "core"
TARGET_TABLE = "scd2_atomic_test"
FULL_TABLE = f"{LAKEHOUSE}.{TARGET_SCHEMA}.{TARGET_TABLE}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Vérification de la classe d’exception Delta
# 
# Cette cellule importe `DeltaConcurrentModificationException` depuis `delta.exceptions` et affiche sa MRO (`__mro__`).
# 
# Objectif : s’assurer que l’exception utilisée par le mécanisme de **retry** est bien disponible et correctement typée.

# CELL ********************

from delta.exceptions import DeltaConcurrentModificationException
print(DeltaConcurrentModificationException.__mro__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Imports du framework et fonctions utilitaires
# 
# Cette cellule importe :
# 
# - `pyspark.sql.functions` sous l’alias `F` pour les agrégations et filtrages,
# - les modèles de configuration (`ColumnContract`, `DataContract`, `TableMapping`) du framework `dp_framework`,
# - le processeur SCD2 (`SCD2Processor`),
# - la fonction `add_technical_columns` pour enrichir les DataFrames avec les colonnes techniques (batch, traitement, etc.).

# CELL ********************

from pyspark.sql import functions as F

from dp_framework.config.models import ColumnContract, DataContract, TableMapping
from dp_framework.processors.scd2 import SCD2Processor
from dp_framework.technical.columns import add_technical_columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. Définition du mapping, du contrat et initialisation de la table de test
# 
# Dans cette cellule :
# 
# 1. Création d’un `TableMapping` simplifié (colonnes `id`, `value`) pour la table de test `scd2_atomic_test`.
# 2. Création du `DataContract` associé avec :
#    - `id` comme clé fonctionnelle,
#    - `value` comme colonne métier.
# 3. **Remise à zéro** de la table cible avec `DROP TABLE IF EXISTS` afin que chaque exécution du notebook reparte d’un état propre.
# 4. Instanciation du `SCD2Processor` sur le lakehouse `LAKEHOUSE`.

# CELL ********************

# Mapping/contrat synthétiques (id, value) — seule la logique SCD2 est testée ici,
# pas un mapping ADP/BDP réel.
mapping = TableMapping(
    name="scd2_atomic_test",
    source_schema="staging",
    source_table="TestSCD2",
    target_schema=TARGET_SCHEMA,
    target_table=TARGET_TABLE,
    source_system="TEST",
    dp_type="ADP",
    load_mode="incremental",
    scd_type=2,
)

contract = DataContract(
    table_name="scd2_atomic_test",
    columns=[
        ColumnContract(name="id", source_name="id", type="Integer", functional_key=True),
        ColumnContract(name="value", source_name="value", type="String"),
    ],
)

# Repart de zéro à chaque exécution complète du notebook.
spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")

proc = SCD2Processor(spark, LAKEHOUSE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5. Premier chargement (batch-1) – création initiale des versions SCD2
# 
# Scénario :
# 
# - Création d’un DataFrame `df1` avec deux lignes : `(1, "a")`, `(2, "b")`.
# - Ajout des colonnes techniques via `add_technical_columns` pour le batch `batch-1` / process `proc-1`.
# - Appel à `proc.write(...)` pour écrire dans la table SCD2.
# 
# Vérification :
# 
# - On attend **2 insertions** et **0 mise à jour** (`(2, 0)`), puisque la table est vide.
# - L’assertion garantit que le premier chargement crée bien seulement des versions initiales.

# CELL ********************

df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
df1 = add_technical_columns(df1, "batch-1", "proc-1")

metrics_a = proc.write(df1, mapping, contract, "batch-1", "proc-1")
print(metrics_a)
assert (metrics_a.rows_inserted, metrics_a.rows_updated) == (2, 0), "Premier chargement KO"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 6. Rejeu du même dataset (batch-2) – test d’idempotence
# 
# Scénario :
# 
# - Réutilisation de `df1` (mêmes données que le batch-1).
# - Lancement d’un nouveau chargement avec `batch-2` / `proc-2`.
# 
# Vérification :
# 
# - On attend **0 insertion** et **0 mise à jour** (`(0, 0)`), car le jeu de données est identique à l’existant.
# - Ce test assure que le processeur SCD2 est **idempotent** sur un même jeu de données.

# CELL ********************

metrics_b = proc.write(df1, mapping, contract, "batch-2", "proc-2")
print(metrics_b)
assert (metrics_b.rows_inserted, metrics_b.rows_updated) == (0, 0), "Le rejeu ne doit créer aucune version"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 7. Chargement avec changement et nouvelle clé (batch-3)
# 
# Scénario :
# 
# - Création d’un DataFrame `df2` avec :
#   - `id=1, value="a-modified"` (clé existante, valeur modifiée),
#   - `id=3, value="c"` (nouvelle clé).
# - Ajout des colonnes techniques pour `batch-3` / `proc-3`.
# - Écriture via `proc.write(...)`.
# 
# Vérification :
# 
# - Pour `id=1`, SCD2 doit :
#   - **fermer** l’ancienne version courante,
#   - **insérer** une nouvelle version.
# - Pour `id=3`, une nouvelle version courante est insérée.
# - On attend donc **2 insertions** et **1 mise à jour** (`(2, 1)`).

# CELL ********************

df2 = spark.createDataFrame([(1, "a-modified"), (3, "c")], ["id", "value"])
df2 = add_technical_columns(df2, "batch-3", "proc-3")

metrics_c = proc.write(df2, mapping, contract, "batch-3", "proc-3")
print(metrics_c)
# id=1 modifié -> 1 fermeture + 1 nouvelle version ; id=3 nouveau -> 1 insertion
assert (metrics_c.rows_inserted, metrics_c.rows_updated) == (2, 1), "Merge CHANGED+NEW KO"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 8. Vérification de l’historique Delta (DESCRIBE HISTORY)
# 
# Cette cellule :
# 
# 1. Récupère l’historique Delta de la table `FULL_TABLE` via `DESCRIBE HISTORY`.
# 2. Affiche les colonnes principales (`version`, `timestamp`, `operation`, `operationMetrics`).
# 3. Construit la liste des opérations dans l’ordre des versions.
# 
# Objectif :
# 
# - S’assurer que **chaque exécution applicative** produit **exactement une opération `MERGE`**.
# - Avant le correctif, chaque run pouvait générer un `MERGE` + un `WRITE` pour les clés existantes, ce qui n’est plus attendu.

# CELL ********************

history = spark.sql(f"DESCRIBE HISTORY {FULL_TABLE}") \
    .select("version", "timestamp", "operation", "operationMetrics")
display(history)

# Chaque run applicatif doit produire exactement 1 ligne 'MERGE'
# (avant le fix : 1 MERGE + 1 WRITE séparés par run modifiant des clés existantes).
ops = [r.operation for r in history.orderBy("version").collect()]
print(ops)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 9. Contrôle d’intégrité SCD2 – unicité de la version courante
# 
# Cette cellule contrôle la cohérence des versions SCD2 :
# 
# 1. Lecture de la table `FULL_TABLE`.
# 2. Agrégation par `id` du nombre de lignes avec `_is_current = 1`.
# 3. Filtre des cas où `nb_current` est différent de 1.
# 
# Vérification :
# 
# - La DataFrame `anomalies` est affichée pour inspection.
# - On impose que `anomalies.count() == 0` :
#   - **aucune clé** ne doit avoir 0 version courante,
#   - **aucune clé** ne doit avoir plusieurs versions courantes simultanées.

# CELL ********************

anomalies = (
    spark.table(FULL_TABLE)
    .groupBy("id")
    .agg(F.sum(F.col("_is_current").cast("int")).alias("nb_current"))
    .filter("nb_current != 1")
)
display(anomalies)
assert anomalies.count() == 0, "Une clé a 0 ou plusieurs versions courantes !"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 10. Visualisation de la table SCD2 ordonnée
# 
# Affiche le contenu complet de la table SCD2 de test en ordonnant par :
# 
# - `id` (clé fonctionnelle),
# - `_valid_from` (date de début de validité).
# 
# Cela permet de vérifier visuellement la chronologie des versions pour chaque clé.

# CELL ********************

display(spark.table(FULL_TABLE).orderBy("id", "_valid_from"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 11. Test du mécanisme de retry en cas de `DeltaConcurrentModificationException`
# 
# Cette cellule simule des conflits de concurrence sur le `MERGE` afin de tester le mécanisme de **retry** intégré dans `SCD2Processor` :
# 
# 1. Création d’un DataFrame `df3` avec `id=2, value="b-modified"`.
# 2. Enrichissement avec les colonnes techniques (`batch-4`, `proc-4`).
# 3. Sauvegarde de la méthode originale `SCD2Processor._execute_merge` dans `original_execute`.
# 4. Définition d’une fonction `flaky_execute` qui :
#    - incrémente un compteur d’appels,
#    - lève `DeltaConcurrentModificationException` pour les deux premiers appels,
#    - appelle `original_execute` au 3ᵉ appel.
# 5. Usage de `unittest.mock.patch` pour remplacer `_execute_merge` par `flaky_execute` le temps du test.
# 6. Appel de `proc.write(...)` avec `df3`.
# 
# Vérifications :
# 
# - Le compteur `call_count['n']` doit être égal à 3 (2 échecs simulés + 1 succès).
# - Les métriques retournées doivent refléter une modification SCD2 classique : **1 insertion** + **1 mise à jour** (`(1, 1)`).
# 
# Ce test valide que le processeur sait **réessayer automatiquement** en cas de conflit Delta concurrent.


# CELL ********************

from unittest.mock import patch
from delta.exceptions import DeltaConcurrentModificationException

df3 = spark.createDataFrame([(2, "b-modified")], ["id", "value"])
df3 = add_technical_columns(df3, "batch-4", "proc-4")

call_count = {"n": 0}
original_execute = SCD2Processor._execute_merge

def flaky_execute(self, dt, staging, keys, batch_id, processing_id, effective_at):
    call_count["n"] += 1
    if call_count["n"] < 3:
        # `desc` seul ne suffit pas : CapturedException exige `desc` ET `stackTrace`
        # (sinon AssertionError avant même de lever l'exception voulue).
        raise DeltaConcurrentModificationException("conflit simulé", "stack trace simulée")
    return original_execute(self, dt, staging, keys, batch_id, processing_id, effective_at)

with patch.object(SCD2Processor, "_execute_merge", flaky_execute):
    metrics_d = proc.write(df3, mapping, contract, "batch-4", "proc-4")

print(f"Tentatives avant succès : {call_count['n']}")
print(metrics_d)
assert call_count["n"] == 3, "Le retry n'a pas été déclenché comme attendu"
assert (metrics_d.rows_inserted, metrics_d.rows_updated) == (1, 1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 12. Visualisation finale du contenu de la table de test
# 
# Affiche les premières lignes (jusqu’à 1000) de la table `FULL_TABLE` telle qu’elle se présente après l’ensemble des scénarios SCD2 (batches 1 à 4).
# 
# Objectif : fournir une vue d’ensemble pour une validation visuelle finale.

# CELL ********************

display(spark.table(FULL_TABLE).limit(1000))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 13. Nettoyage – suppression de la table de test
# 
# Dernière étape du test d’intégration :
# 
# - Suppression de la table `FULL_TABLE` avec `DROP TABLE IF EXISTS`.
# 
# Cela permet de laisser l’environnement propre après exécution, en évitant d’accumuler des tables de test persistantes.

# CELL ********************

spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
