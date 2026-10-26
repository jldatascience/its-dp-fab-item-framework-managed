# Résultats de l’analyse du repo Framework

Ces trois fichiers sont issus de l’analyse du repo Framework sur la branche `develop` :

- `inventaire_item-framework.json` ;
- `activites_item-framework.csv` ;
- `toutes_refs_item-framework.csv`.

Ensemble, ils permettent d’identifier les objets Microsoft Fabric présents dans le code, les dépendances entre les pipelines et les notebooks, ainsi que les références et identifiants à variabiliser entre DEV, ITG, PPR et PRD.

## 1. `inventaire_item-framework.json` : vue complète du repo

Il s’agit du fichier principal de l’inventaire. Il recense les items Fabric détectés dans `develop`, avec pour chacun :

- son nom ;
- son type, par exemple `Notebook`, `DataPipeline`, `Environment` ou `VariableLibrary` ;
- sa branche ;
- son emplacement dans le repo.

Par exemple, il identifie :

- `nb_run_table` comme un `Notebook` ;
- `pl_run_table` comme un `DataPipeline` ;
- `vl_core_config` comme une `VariableLibrary`.

Le fichier contient également les activités référencées et les GUID repérés dans le code.

> **En clair : qu’est-ce qui existe actuellement dans le repo Framework ?**

## 2. `activites_item-framework.csv` : dépendances entre pipelines et notebooks

Ce fichier recense les activités qui appellent d’autres éléments Fabric. Il permet notamment d’identifier :

- le pipeline contenant l’activité ;
- le nom de l’activité appelée ;
- le type d’appel, par exemple `TridentNotebook` ou `InvokePipeline` ;
- le `workspaceId` associé ;
- le `pipelineId` ou le `notebookId` ciblé.

> **En clair : qu’est-ce que chaque pipeline appelle ?**

## 3. `toutes_refs_item-framework.csv` : tous les GUID détectés

Ce fichier centralise les GUID repérés dans le code du repo Framework. Pour chaque référence, il indique :

- l’item concerné ;
- le type de l’item ;
- la branche analysée ;
- le fichier dans lequel le GUID apparaît ;
- la propriété ou la clé concernée ;
- le GUID détecté ;
- sa classification.

> **En clair : où se trouvent les IDs en dur et lesquels doivent être remplacés ou variabilisés ?**

## 4. Validation du périmètre SANDBOX

L’analyse de `develop` fait apparaître **28 lignes classées `SANDBOX (à remplacer)`**, soit exactement les **28 activités du périmètre analysé**.

Toutes pointent actuellement vers le workspace sandbox suivant :

```text
42463408-0fbe-47d5-93d1-d51019527925
```

Le périmètre se répartit ainsi :

- `pl_get_ws_lh_ids` : 1 activité ;
- `pl_run_table` : 1 activité ;
- `pl_src_to_arc` : 3 activités ;
- `pl_src_to_stg` : 3 activités ;
- `pl_stg_to_str` : 3 activités ;
- pipelines `log_load_*` : 3 activités ;
- pipelines `log_processing_*` : 3 activités ;
- pipelines `log_sequence_*` : 3 activités ;
- `pl_load_object` : 8 activités.

```text
1 + 1 + 3 + 3 + 3 + 3 + 3 + 3 + 8 = 28 activités
```

Le périmètre inventorié ne contient plus d’éléments suffixés `_v1`.

## 5. Point crucial : 28 lignes SANDBOX ne signifient pas 28 règles

Les 28 lignes classées `SANDBOX` portent toutes sur une clé de la forme :

```text
properties.activities[n].typeProperties.workspaceId
```

Elles représentent donc uniquement les références au **workspace sandbox**.

Pour chaque activité, il existe également une seconde référence vers l’item appelé :

- `pipelineId` pour une activité `InvokePipeline` ;
- `notebookId` pour une activité `TridentNotebook`.

Ces identifiants ne commencent pas par le GUID du workspace sandbox. Ils correspondent aux GUID réels des items présents dans ce workspace et sont classés `autre (workspace/item/connexion)` dans l’inventaire. Exemples observés :

```text
6eda81c3-0ee5-48e6-a533-7f6c553a34d6
30e913fd-7a41-45b5-8899-1bae35a0d724
93e54804-908c-4505-9186-5ffdddb29e14
```

Ces `pipelineId` et `notebookId` doivent eux aussi être remplacés par les GUID DEV des items correspondants. Sinon, après déploiement, les activités continueraient à cibler les items du sandbox.

## 6. Décompte complet des règles `parameter.yml`

| Propriété | Valeur actuelle | Classification dans l’inventaire | Remplacement attendu |
|---|---|---|---|
| `workspaceId` | `42463408-0fbe-47d5-93d1-d51019527925` | `SANDBOX (à remplacer)` : 28 occurrences | GUID du workspace DEV |
| `pipelineId` ou `notebookId` | GUID réel de l’item sandbox | `autre (workspace/item/connexion)` : 28 occurrences | GUID DEV de l’item correspondant |

Le décompte complet est donc :

```text
28 règles workspaceId
+ 28 règles pipelineId / notebookId
= 56 règles parameter.yml
```

Le filtre `SANDBOX` ne montre que la moitié des remplacements nécessaires, celle des `workspaceId`. La moitié portant sur les identifiants des items reste indispensable.

Sur la base de ce périmètre, le template `parameter.yml` comportant **56 règles** est cohérent avec les **28 activités** inventoriées. Il ne doit pas être réduit à 28 règles.

## 7. Résultat concret

L’analyse permet de confirmer les points suivants :

- le GUID complet du workspace sandbox est `42463408-0fbe-47d5-93d1-d51019527925` ;
- 28 activités actives sont concernées ;
- chaque activité nécessite deux remplacements : le workspace et l’item appelé ;
- le besoin total est donc de 56 règles ;
- aucun élément `_v1` n’apparaît dans le périmètre inventorié.

## 8. Processus global

```text
Repo Framework / branche develop
                |
                v
    01_inventaire_github.py
                |
                v
Inventaire des items
    -> inventaire_item-framework.json

Relations entre les items
    -> activites_item-framework.csv

GUID présents dans le code
    -> toutes_refs_item-framework.csv
                |
                v
28 références workspaceId
+ 28 références pipelineId / notebookId
                |
                v
56 règles parameter.yml
                |
                v
Référencement dynamique par environnement
DEV -> ITG -> PPR -> PRD
```

Le script `01_inventaire_github.py` produit ainsi une **photo du contenu Git du repo Framework** avant la correspondance avec les ressources présentes dans Microsoft Fabric et l’application des règles de variabilisation.
