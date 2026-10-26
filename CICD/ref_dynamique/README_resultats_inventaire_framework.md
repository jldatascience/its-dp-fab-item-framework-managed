# Résultats de l’analyse du repo Framework

Ces trois fichiers sont le résultat de l’analyse du repo Framework sur la branche `develop`.

Ensemble, ils permettent d’identifier :

- les objets Microsoft Fabric présents dans le code ;
- les dépendances entre les pipelines et les notebooks ;
- les références et identifiants codés dans les fichiers du repo ;
- les identifiants qui devront potentiellement être remplacés ou variabilisés entre les environnements DEV, ITG, PPR et PRD.

## 1. `inventaire_item-framework.json` : vue complète du repo

Il s’agit du fichier principal de l’inventaire.

Il recense les items Fabric détectés dans la branche `develop`, avec pour chacun :

- son nom ;
- son type, par exemple `Notebook`, `DataPipeline`, `Environment` ou `VariableLibrary` ;
- sa branche ;
- son emplacement dans le repo.

Par exemple, le fichier identifie :

- `nb_run_table` comme un `Notebook` ;
- `pl_run_table` comme un `DataPipeline` ;
- `vl_core_config` comme une `VariableLibrary`.

Le fichier contient également les activités référencées et les GUID repérés dans le code.

> **En clair : qu’est-ce qu’il y a actuellement dans le repo Framework ?**

## 2. `activites_item-framework.csv` : dépendances entre pipelines et notebooks

Ce fichier se concentre sur les activités qui appellent d’autres éléments Fabric.

Il permet notamment d’identifier :

- le pipeline contenant l’activité ;
- le nom de l’activité appelée ;
- le type d’appel, par exemple `TridentNotebook` ou `InvokePipeline` ;
- le `workspaceId` associé ;
- le `pipelineId` ou le `notebookId` ciblé.

Par exemple, le fichier indique que `pl_run_table` contient une activité `nb_run_table` de type `TridentNotebook`, avec son `notebookId`. Il présente également plusieurs activités `InvokePipeline` avec leur `pipelineId`.

Ces informations sont particulièrement utiles pour l’industrialisation, car les références entre les items doivent rester cohérentes lors du passage d’un environnement à un autre.

> **En clair : qu’est-ce que chaque pipeline appelle ?**

## 3. `toutes_refs_item-framework.csv` : tous les GUID détectés

Ce fichier centralise les GUID repérés dans le code du repo Framework.

Pour chaque référence, il indique :

- l’item concerné ;
- le type de l’item ;
- la branche analysée ;
- le fichier dans lequel le GUID apparaît ;
- la propriété ou la clé concernée ;
- le GUID détecté ;
- sa classification.

Ce fichier est particulièrement utile pour le travail de variabilisation.

Par exemple, le GUID suivant est classé `SANDBOX (à remplacer)` dans plusieurs propriétés `workspaceId` :

```text
42463408-0fbe-47d5-93d1-d51019527925
```

Le fichier détecte également :

- des références au workspace MDS DEV ;
- une connexion Fabric Data Pipelines commune ;
- d’anciennes connexions marquées comme archives ;
- des `pipelineId`, `notebookId`, `environmentId`, `lakehouseId`, `workspaceId` et autres identifiants techniques.

> **En clair : où se trouvent les IDs en dur et lesquels doivent potentiellement être remplacés ou variabilisés ?**

## Processus global

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
Comparaison avec les ressources réellement présentes dans Fabric
                |
                v
Construction du référencement dynamique par environnement
DEV -> ITG -> PPR -> PRD
```

Le script `01_inventaire_github.py` sert donc à produire une **photo du contenu Git du repo Framework** avant d’effectuer la correspondance avec les ressources présentes dans Microsoft Fabric.

## Fichiers associés

- `inventaire_item-framework.json`
- `activites_item-framework.csv`
- `toutes_refs_item-framework.csv`
