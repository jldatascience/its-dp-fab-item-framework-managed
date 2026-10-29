# Changements — its-dp-fab-item-framework-managed

Changements notables de ce dépôt (configuration de déploiement et pipelines), du plus récent
au plus ancien. Format inspiré de [Keep a Changelog](https://keepachangelog.com/fr/1.1.0/).
Les évolutions du socle commun sont décrites dans `CHANGELOG.md` de `its-dp-fab-cicd-common`.

## [Unreleased]

### Fixed

- `CICD/pipelines/CD.yml` : template `pipelines/templates/cd-pipeline.yml@cicd_common` →
  `CICD/pipelines/templates/cd-pipeline.yml@cicd_common` (emplacement réel dans le socle).
- `CICD/pipelines/PR-Validation.yml` : template `pipelines/templates/validate-pipeline.yml@cicd_common` →
  `CICD/pipelines/templates/validate-pipeline.yml@cicd_common`.
- `CICD/deployment/config.yml` : `repository_directory` `".."` → `"../../fabric"`. Le chemin est
  résolu depuis le dossier de `config.yml` (`CICD/deployment/`) : `".."` désignait `CICD/`.
  - Items découverts sous `fabric/` ; dossiers exclus inchangés (`folder_exclude_regex` relatif à `fabric/`).
- `CICD/ref_dynamique/templates/config.yml` : même correction de `repository_directory`.
- En-têtes des fichiers `CICD/deployment/*.yml` et README alignés sur l'arborescence `CICD/`.

### Added

- `CHANGES.md` (ce fichier).
