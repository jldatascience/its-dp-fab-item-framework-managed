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
# META       "environmentId": "b64c98fc-8d6c-4627-a5aa-6eddb8a07d50",
# META       "workspaceId": "a699158a-f604-431c-b9f4-aff776a02c5d"
# META     }
# META   }
# META }

# CELL ********************

from typing import Optional
import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")

import yaml
from pyspark.sql import SparkSession


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

# CELL ********************

"""Modèles de configuration — Dataclasses pour le mapping et les contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class SourceConnection:
    """Connexion vers une source de données (lakehouse Fabric, SQL endpoint, etc.)."""

    name: str
    type: str  # lakehouse | synapsesql
    artifact: str  # nom de l'artifact Fabric (lakehouse, etc.)
    workspace_id: Optional[str] = None  # None = même workspace Fabric, sinon GUID
    default_schema: str = "dbo"


@dataclass(frozen=True)
class TableMapping:
    """Configuration de mapping source → cible pour une table."""

    name: str
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    source_system: str
    dp_type: str  # ADP | BDP
    load_mode: str  # full | incremental
    scd_type: int = 2  # 1 | 2
    watermark_column: Optional[str] = None
    soft_delete: bool = False
    replay_from_date: Optional[str] = None
    active: bool = True
    source_connection: Optional[str] = None  # nom de connexion dans connections/sources.yml (ADP et BDP)


@dataclass(frozen=True)
class ColumnContract:
    """Définition d'une colonne dans le data contract."""

    name: str
    source_name: str
    type: str  # String | Integer | Long | Double | Date | Timestamp | Boolean
    functional_key: bool = False
    uniqueness_check: bool = False
    completeness_check: bool = False
    validity_check: bool = False
    format_type: Optional[str] = None  # REGEX | DATE
    format_value: Optional[str] = None


@dataclass(frozen=True)
class DataContract:
    """Data contract complet d'une table (métadonnées + colonnes)."""

    table_name: str
    columns: list[ColumnContract] = field(default_factory=list)

    @property
    def functional_keys(self) -> list[str]:
        return [c.name for c in self.columns if c.functional_key]

    @property
    def completeness_columns(self) -> list[str]:
        return [c.name for c in self.columns if c.completeness_check]

    @property
    def validity_columns(self) -> list[ColumnContract]:
        return [c for c in self.columns if c.validity_check]


@dataclass
class ProcessingResult:
    """Résultat d'un traitement pipeline pour une table."""

    table_name: str
    status: str  # success | failure
    rows_read: int = 0
    rows_valid: int = 0
    rows_rejected: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    error_message: Optional[str] = None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""Lecteur YAML — Charge le mapping et les contracts depuis le lakehouse metadata."""

from __future__ import annotations

import logging

import yaml
from pyspark.sql import SparkSession

# Logger Python standard — affiche les traces de lecture YAML dans la console / notebook
_log = logging.getLogger(__name__)


class YamlReader:
    """Lit les fichiers YAML de configuration depuis le lakehouse metadata.

    Sur Fabric, les fichiers sont dans la section Files du lakehouse:
        abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files/metadata/

    Usage:
        reader = YamlReader(spark, metadata_root)
        mapping     = reader.read_mapping("ma_table")
        contract    = reader.read_contract("ma_table")   # ADP
        connections = reader.read_connections()           # BDP uniquement
    """

    def __init__(self, spark: SparkSession, metadata_path: str) -> None:
        """
        Args:
            spark: SparkSession active (passée depuis le notebook appelant).
            metadata_path: Chemin racine des fichiers metadata.
                Ex: "abfss://...Files/metadata/adp"
        """
        self._spark = spark
        self._metadata_path = metadata_path.rstrip("/")
        self._mappings_cache: dict[str, TableMapping] | None = None

    def read_mapping(self, table_name: str) -> TableMapping:
        """Retourne le TableMapping d'une table depuis tables_mapping.yml.

        Lève ValueError si la table n'est pas présente dans le fichier.
        """
        _log.debug("[YamlReader] Lecture du mapping pour la table '%s'", table_name)
        mappings = self._load_all_mappings()
        if table_name not in mappings:
            _log.error(
                "[YamlReader] Table '%s' introuvable dans tables_mapping.yml. Disponibles : %s",
                table_name, list(mappings.keys()),
            )
            raise ValueError(
                f"Table '{table_name}' non trouvée dans tables_mapping.yml. "
                f"Tables disponibles: {list(mappings.keys())}"
            )
        _log.debug("[YamlReader] Mapping trouvé pour la table '%s'", table_name)
        return mappings[table_name]

    def read_contract(self, table_name: str) -> DataContract:
        """Retourne le DataContract d'une table depuis contracts/<table_name>.yml."""
        path = f"{self._metadata_path}/contracts/{table_name}.yml"
        _log.info("[YamlReader] Lecture du contract '%s' depuis %s", table_name, path)
        content = self._read_yaml_file(path)
        contract = self._parse_contract(content)
        _log.info("[YamlReader] Contract '%s' chargé — %d colonne(s)", table_name, len(contract.columns))
        return contract

    def list_active_tables(self) -> list[str]:
        """Retourne les noms des tables marquées `active: true` dans le mapping."""
        mappings = self._load_all_mappings()
        active = [name for name, m in mappings.items() if m.active]
        _log.info("[YamlReader] %d table(s) active(s) sur %d au total", len(active), len(mappings))
        return active

    def read_connections(self) -> dict[str, SourceConnection]:
        """Retourne les SourceConnection depuis connections/sources.yml."""
        path = f"{self._metadata_path}/connections/sources.yml"
        _log.info("[YamlReader] Lecture des connexions depuis %s", path)
        content = self._read_yaml_file(path)
        connections = self._parse_connections(content)
        _log.info("[YamlReader] %d connexion(s) chargée(s) : %s", len(connections), list(connections.keys()))
        return connections

    # ------------------------------------------------------------------
    # Méthodes privées — lecture et parsing des fichiers YAML
    # ------------------------------------------------------------------

    def _load_all_mappings(self) -> dict[str, TableMapping]:
        """Charge et met en cache tables_mapping.yml (appelé par read_mapping et list_active_tables)."""
        # Retourner le cache si déjà chargé — évite de relire le YAML à chaque appel
        if self._mappings_cache is not None:
            _log.debug("[YamlReader] Cache mappings utilisé (%d table(s))", len(self._mappings_cache))
            return self._mappings_cache

        path = f"{self._metadata_path}/mappings/tables_mapping.yml"
        _log.info("[YamlReader] Chargement de tables_mapping.yml depuis %s", path)
        content = self._read_yaml_file(path)

        # Parser chaque entrée de tables[] en appliquant les défauts du bloc data_product
        dp_defaults = content.get("data_product", {})
        mappings: dict[str, TableMapping] = {}
        for tbl in content.get("tables", []):
            mapping = self._parse_mapping(tbl, dp_defaults)
            mappings[mapping.name] = mapping

        _log.info("[YamlReader] %d table(s) chargée(s) depuis tables_mapping.yml", len(mappings))
        self._mappings_cache = mappings
        return mappings

    @staticmethod
    def _parse_mapping(tbl: dict, dp_defaults: dict | None = None) -> TableMapping:
        """Construit un TableMapping depuis un dict YAML (une entrée de tables[])."""
        defaults = dp_defaults or {}
        return TableMapping(
            name=tbl["name"],
            source_schema=tbl["source_schema"],
            source_table=tbl["source_table"],
            target_schema=tbl.get("target_schema", "storage"),
            target_table=tbl.get("target_table", tbl["name"]),
            source_system=tbl.get("source_system", defaults.get("source_system", "unknown")),
            dp_type=tbl.get("dp_type", defaults.get("dp_type", "ADP")),
            load_mode=tbl.get("load_mode", "full"),
            scd_type=tbl.get("scd_type", 2),
            watermark_column=tbl.get("watermark_column"),
            soft_delete=tbl.get("soft_delete", False),
            replay_from_date=tbl.get("replay_from_date"),
            active=tbl.get("active", True),
            source_connection=tbl.get("source_connection"),  # référence vers connections/sources.yml
        )

    @staticmethod
    def _parse_connections(content: dict) -> dict[str, SourceConnection]:
        """Construit un dict de SourceConnection depuis un dict sources.yml."""
        connections: dict[str, SourceConnection] = {}
        for name, raw in content.get("sources", {}).items():
            connections[name] = SourceConnection(
                name=name,
                type=raw["type"],
                artifact=raw["artifact"],
                workspace_id=raw.get("workspace_id"),
                default_schema=raw.get("default_schema", "dbo"),
            )
        return connections

    @staticmethod
    def _parse_contract(content: dict) -> DataContract:
        """Construit un DataContract depuis un dict YAML (contenu d'un fichier contract)."""
        contract_meta = content.get("contract", {})
        table_name = contract_meta.get("table_name")
        if not table_name:
            raise ValueError("Champ requis 'contract.table_name' manquant dans le fichier contract YAML.")
        # Construire la liste de ColumnContract depuis la section 'columns' du YAML
        columns = [
            ColumnContract(
                name=col["name"],
                source_name=col.get("source_name", col["name"]),
                type=col["type"],
                functional_key=col.get("functional_key", False),
                uniqueness_check=col.get("uniqueness_check", False),
                completeness_check=col.get("completeness_check", False),
                validity_check=col.get("validity_check", False),
                format_type=col.get("format_type"),
                format_value=col.get("format_value"),
            )
            for col in content.get("columns", [])
        ]
        _log.debug("[YamlReader] Contract '%s' parsé — %d colonne(s)", table_name, len(columns))
        return DataContract(table_name=table_name, columns=columns)

    def _read_yaml_file(self, path: str) -> dict:
        """Lit un fichier YAML via Spark (chemin ABFSS ou local)."""
        _log.debug("[YamlReader] Lecture du fichier YAML : %s", path)
        lines = self._spark.read.text(path).select("value").collect()
        yaml_content = "\n".join(row["value"] for row in lines)
        result = yaml.safe_load(yaml_content)
        _log.debug("[YamlReader] Fichier YAML chargé avec succès : %s", path)
        return result



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Lire un data contract YAML spécifique via le YamlReader existant

# Racine des métadonnées ADP dans le Lakehouse
metadata_root = "abfss://a699158a-f604-431c-b9f4-aff776a02c5d@onelake.dfs.fabric.microsoft.com/22885ae1-ec38-4913-bd38-c73c8dc628e3/Files/metadata/adp"

# On utilise l'API publique du YamlReader au lieu de fonctions globales inexistantes
reader = YamlReader(spark, metadata_root)

# Charge le contrat de la table "groupementgup" (fichier contracts/groupementgup.yml)
data_contract = reader.read_contract("groupementgup")

data_contract.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Lecture de tables_mapping.yml comme pour le contrat

# On réutilise metadata_root défini précédemment
reader = YamlReader(spark, metadata_root)

# Charge l'ensemble des mappings depuis tables_mapping.yml
mappings = reader.read_mapping("groupementgup")  # dict[str, TableMapping]

# Affiche le dict complet pour inspection
mappings


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

connections = reader.read_connections()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""Reader — Lecture des données source (staging pour ADP, DSO pour BDP)."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# Logger Python standard — affiche les traces de lecture de la source
_log = logging.getLogger(__name__)


class SourceReader:
    """Lit les données source selon le type de connexion défini dans sources.yml.

    Deux modes de lecture sont supportés:
    - ``lakehouse`` : lit depuis un lakehouse Fabric via ``spark.table()`` (Delta Lake).
      Utilisé pour les ADP qui lisent depuis le Lakehouse Staging.
    - ``synapsesql`` : lit depuis le SQL Analytics Endpoint d'un lakehouse via
      ``spark.read.synapsesql()``. Utilisé pour les BDP qui lisent depuis le DSO.

    Le type de lecture est déterminé par ``connection.type``, résolu depuis les
    connexions chargées en amont (``connections/sources.yml``).
    """

    def __init__(
        self,
        spark: SparkSession,
        lakehouse: str,
        connections: dict[str, SourceConnection],
    ) -> None:
        """
        Args:
            spark: SparkSession active.
            lakehouse: Nom du lakehouse cible (ex: "lkh_core").
            connections: Dictionnaire des connexions chargées depuis sources.yml.
        """
        self._spark = spark
        self._lakehouse = lakehouse
        self._connections = connections

    def read(self, mapping: TableMapping, last_watermark: str | None = None) -> DataFrame:
        """Lit les données source avec filtre incrémental optionnel.

        Résout la connexion depuis ``mapping.source_connection``, dispatche vers
        ``read_from_lakehouse()`` ou ``read_from_sql_endpoint()`` selon
        ``connection.type``, puis applique les filtres (watermark, replay).

        Args:
            mapping: Configuration de la table.
            last_watermark: Dernière valeur watermark traitée (mode incrémental).

        Returns:
            DataFrame avec les données source.

        Raises:
            ValueError: Si source_connection est absent, introuvable, ou de type inconnu.
        """
        # Résolution de la connexion et dispatch vers le lecteur approprié
        connection = self._get_connection(mapping)
        _log.info(
            "[SourceReader][%s] Lecture depuis la source — type=%s, artifact=%s",
            mapping.name, connection.type, connection.artifact,
        )

        if connection.type == "lakehouse":
            df = self.read_from_lakehouse(mapping, connection)
        elif connection.type == "synapsesql":
            df = self.read_from_sql_endpoint(mapping, connection)
        else:
            raise ValueError(
                f"Table '{mapping.name}': type de source non supporté '{connection.type}'. "
                "Types supportés : 'lakehouse', 'synapsesql'."
            )

        # Filtre incrémental : ne conserver que les lignes plus récentes que le dernier watermark
        if mapping.load_mode == "incremental" and mapping.watermark_column and last_watermark:
            _log.info(
                "[SourceReader][%s] Filtre incrémental appliqué — colonne=%s, depuis=%s",
                mapping.name, mapping.watermark_column, last_watermark,
            )
            df = df.filter(F.col(mapping.watermark_column) > F.lit(last_watermark))

        # Filtre replay_from_date (mode replay BDP)
        if mapping.replay_from_date:
            _log.info(
                "[SourceReader][%s] Filtre replay_from_date appliqué — depuis=%s",
                mapping.name, mapping.replay_from_date,
            )
            df = df.filter(F.col("_valid_from") >= F.lit(mapping.replay_from_date))

        return df

    def read_from_lakehouse(self, mapping: TableMapping, connection: SourceConnection) -> DataFrame:
        """Lit depuis un lakehouse Fabric via spark.table() (format Delta Lake).

        Utilisé pour les ADP qui lisent depuis le Lakehouse Staging.
        La référence complète est : ``{connection.artifact}.{schema}.{table}``.

        Args:
            mapping: Configuration de la table.
            connection: Connexion lakehouse résolue depuis sources.yml.

        Returns:
            DataFrame brut depuis le lakehouse.
        """
        # Construction de la référence complète {artifact}.{schema}.{table}
        schema = mapping.source_schema or connection.default_schema
        full_table_name = f"{connection.artifact}.{schema}.{mapping.source_table}"
        _log.info("[SourceReader][%s] Lecture Lakehouse — table=%s", mapping.name, full_table_name)
        return self._spark.table(full_table_name)

    def read_from_sql_endpoint(self, mapping: TableMapping, connection: SourceConnection) -> DataFrame:
        """Lit depuis le SQL Analytics Endpoint d'un lakehouse via synapsesql (BDP).

        Sur Fabric, ``spark.read.synapsesql()`` utilise le SQL Analytics Endpoint
        du lakehouse DSO avec l'identité MSI du notebook.
        La référence complète est : ``{connection.artifact}.{schema}.{table}``.

        Args:
            mapping: Configuration de la table.
            connection: Connexion synapsesql résolue depuis sources.yml.

        Returns:
            DataFrame brut depuis le SQL endpoint.
        """
        # Construction de la référence synapsesql {artifact}.{schema}.{table}
        schema = mapping.source_schema or connection.default_schema
        synapsesql_ref = f"{connection.artifact}.{schema}.{mapping.source_table}"
        _log.info("[SourceReader][%s] Lecture SQL Endpoint — référence=%s", mapping.name, synapsesql_ref)
        return self._spark.read.synapsesql(synapsesql_ref)

    # ------------------------------------------------------------------
    # Méthodes privées
    # ------------------------------------------------------------------

    def _get_connection(self, mapping: TableMapping) -> SourceConnection:
        """Résout la connexion depuis le dictionnaire des connexions chargées.

        Args:
            mapping: Configuration de la table (doit avoir source_connection défini).

        Raises:
            ValueError: Si source_connection est absent ou introuvable dans le dict.
        """
        # Récupération du nom de connexion déclaré dans le mapping
        conn_name = mapping.source_connection
        if not conn_name:
            raise ValueError(
                f"Table '{mapping.name}': 'source_connection' non défini dans tables_mapping.yml. "
                "Chaque table doit référencer une entrée de connections/sources.yml."
            )
        conn = self._connections.get(conn_name)
        if conn is None:
            _log.error(
                "[SourceReader][%s] Connexion '%s' introuvable. Disponibles : %s",
                mapping.name, conn_name, list(self._connections.keys()),
            )
            raise ValueError(
                f"Table '{mapping.name}': connexion '{conn_name}' introuvable. "
                f"Connexions disponibles : {list(self._connections.keys())}"
            )
        _log.debug("[SourceReader][%s] Connexion '%s' résolue (type=%s)", mapping.name, conn_name, conn.type)
        return conn


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mappings

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_reader = SourceReader(spark, "lkh_core", connections)
df= data_reader.read(mappings)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""Validation du Data Contract — vérifie structure, types et clé fonctionnelle."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame


# Logger Python standard — affiche les traces de validation de contract
_log = logging.getLogger(__name__)


class ContractViolationError(Exception):
    """Levée quand le Data Contract est violé (erreur bloquante)."""


class ContractValidator:
    """Valide un DataFrame contre le data contract YAML.

    Contrôles effectués (tous bloquants):
    1. Présence: toutes les colonnes du contract existent dans le DataFrame
    2. Types: types Spark compatibles avec les types déclarés
    3. Clé fonctionnelle: au moins une colonne functional_key=true est définie
    """

    # Groupes de compatibilité de types Spark
    # Utilisé par _types_compatible() pour accepter plusieurs variantes d'un même type.
    # Structure : { type_déclaré_dans_le_YAML : ensemble des types Spark réels acceptés }
    #   - La clé est le nom "canonique" tel qu'il peut apparaître dans le contrat YAML.
    #   - Les valeurs sont les types Spark renvoyés par dataType.simpleString() qui sont
    #     considérés comme équivalents (ex. "int" et "integer" sont deux noms du même type).
    #   - Les alias métier sont aussi inclus (ex. "datetime" → groupe "timestamp").
    _TYPE_GROUPS: dict[str, set[str]] = {
        "string": {"string", "varchar", "char"},
        "integer": {"int", "integer", "smallint", "tinyint", "short"},
        "bigint": {"bigint", "long"},
        "long": {"long", "bigint"},
        "double": {"double", "float", "decimal"},
        "date": {"date"},
        "timestamp": {"timestamp", "timestamp_ntz"},
        "datetime": {"timestamp", "timestamp_ntz"},
        "boolean": {"boolean"},
    }

    def validate(self, df: DataFrame, contract: DataContract) -> None:
        """Valide le DataFrame contre le contract. Lève ContractViolationError si violation.

        Args:
            df: DataFrame à valider.
            contract: DataContract chargé depuis le YAML.

        Raises:
            ContractViolationError: Si au moins une violation est détectée.
        """
        _log.info(
            "[ContractValidator] Validation du contract '%s' — %d colonne(s) déclarée(s)",
            contract.table_name, len(contract.columns),
        )

        # Si le contract ne déclare aucune colonne, validation ignorée (contract vide)
        if not contract.columns:
            _log.warning("[ContractValidator] Contract '%s' sans colonnes — validation ignorée", contract.table_name)
            return

        violations: list[str] = []

        # Contrôle 1 — Vérifier qu'au moins une clé fonctionnelle est définie
        if not contract.functional_keys:
            violations.append("Aucune clé fonctionnelle définie (functional_key=true)")

        # Map des colonnes du DataFrame (case-insensitive) pour comparaison robuste
        df_columns_lower = {c.lower(): c for c in df.columns}
        df_schema_map = {f.name.lower(): f for f in df.schema.fields}

        for col_contract in contract.columns:
            col_lower = col_contract.source_name.lower()

            # Contrôle 2 — Présence de la colonne dans le DataFrame
            if col_lower not in df_columns_lower:
                violations.append(f"Colonne requise manquante: '{col_contract.source_name}'")
                continue

            # Contrôle 3 — Compatibilité du type Spark avec le type déclaré
            df_field = df_schema_map.get(col_lower)
            if df_field:
                actual_type = df_field.dataType.simpleString().lower()
                expected_type = col_contract.type.lower().strip()
                if not self._types_compatible(actual_type, expected_type):
                    violations.append(
                        f"Type incompatible pour '{col_contract.source_name}': "
                        f"attendu '{col_contract.type}', trouvé '{actual_type}'"
                    )

        if violations:
            _log.error(
                "[ContractValidator] %d violation(s) détectée(s) pour '%s' : %s",
                len(violations), contract.table_name, violations,
            )
            msg = (
                f"Contract violation pour '{contract.table_name}' "
                f"({len(violations)} violation(s)):\n"
            )
            msg += "\n".join(f"  - {v}" for v in violations)
            raise ContractViolationError(msg)

        _log.info("[ContractValidator] Contract '%s' validé sans violation", contract.table_name)

    def _types_compatible(self, actual: str, expected: str) -> bool:
        """Vérifie la compatibilité entre le type Spark réel et le type déclaré.

        Deux types sont compatibles s'ils appartiennent au même groupe de
        ``_TYPE_GROUPS`` ou s'ils sont strictement identiques. On n'utilise
        volontairement pas ``startswith`` en fallback : cela causerait des faux
        positifs (ex. "bool" vs "boolean", "int" vs "interval", etc.).
        """
        expected_lower = expected.lower()
        # Normalise les types paramétrés (ex. "decimal(18,2)" → "decimal")
        # pour permettre la correspondance exacte dans les sets de _TYPE_GROUPS
        if "(" in actual:
            actual = actual[: actual.index("(")]
        for group_name, group_types in self._TYPE_GROUPS.items():
            if expected_lower == group_name or expected_lower in group_types:
                return actual in group_types or actual == group_name
        # Aucun groupe déclaré : exiger une correspondance exacte pour éviter
        # les faux positifs (pas de startswith).
        return actual == expected_lower



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _apply_column_mapping(df: DataFrame, contract: DataContract) -> DataFrame:
    """Renomme les colonnes source vers les noms cibles du contract."""
    renamed_count = 0
    for col_contract in contract.columns:
        # Renommage uniquement si le nom source diffère du nom cible
        if col_contract.source_name != col_contract.name:
            if col_contract.source_name in df.columns:
                _log.debug(
                    "[Pipeline] Renommage colonne : '%s' → '%s'",
                    col_contract.source_name,
                    col_contract.name,
                )
                df = df.withColumnRenamed(col_contract.source_name, col_contract.name)
                renamed_count += 1
            else:
                # La colonne source est absente du DataFrame — ignorée silencieusement
                _log.warning(
                    "[Pipeline] Colonne source '%s' absente du DataFrame — renommage ignoré",
                    col_contract.source_name,
                )
    _log.info("[Pipeline] Mapping de colonnes appliqué — %d colonne(s) renommée(s)", renamed_count)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging
_log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
table_name = mappings.name
_log.info("[Pipeline][%s] Lecture de la source (watermark=%s)", table_name, None)
df = data_reader.read(mappings, None)
rows_read = df.count()
_log.info("[Pipeline][%s] Lecture terminée — %d ligne(s) lue(s)", table_name, rows_read)

# Arrêt anticipé si la source est vide : rien à traiter
if rows_read == 0:
    _log.info("[Pipeline][%s] Source vide — pipeline terminé sans traitement", table_name)
    result = ProcessingResult(table_name=table_name, status="success", rows_read=0)
    _logger.log_processing_end(batch_id, processing_id, result)
    result
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_dp_type = "ADP"
_contract_validator = ContractValidator()
if _dp_type == "ADP":
    _log.info("[Pipeline][%s] Validation du contract en cours...", table_name)
    _contract_validator.validate(df, data_contract)
    _log.info("[Pipeline][%s] Contract validé avec succès", table_name)

    _log.info("[Pipeline][%s] Application du mapping de colonnes", table_name)
    df = _apply_column_mapping(df, data_contract)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Court-circuit : écriture directe en overwrite
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lkh_storage.storage.groupementgup")
)

print(f"✓ {df.count()} lignes écrites dans lkh_storage.core.groupementgup")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""Contrôles de qualité technique — unicité, complétude, format, doublons.

Rôle dans le pipeline
---------------------
Après validation du contract (types/colonnes attendus), on applique des
**contrôles de qualité fonctionnels** sur les données elles-mêmes. Une ligne
peut être valide côté schéma mais violer des règles métier — par exemple :

- Un champ obligatoire est nul (``completeness``).
- Un email ne respecte pas la regex configurée (``validity`` REGEX).
- Une date n'est pas au format attendu (``validity`` DATE).
- Deux lignes ont la même clé fonctionnelle (``uniqueness``).
- Deux lignes sont entièrement identiques (``duplicate_row``).

Les lignes rejetées ne bloquent **pas** le pipeline : elles sont extraites
dans un DataFrame séparé qui sera écrit dans ``rejects.rej_<table>`` par
le ``RejectionHandler``. Le pipeline continue avec les lignes valides.

Chaque rejet est tracé dans la colonne technique ``_rejection_reason`` sous
la forme ``reason_type:column_name`` (ex : ``completeness:email``). Une même
ligne peut cumuler plusieurs raisons (concaténées avec ``|``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Logger Python standard — sortie dans la console / notebook Fabric.
_log = logging.getLogger(__name__)


@dataclass
class QualityResult:
    """Résultat des contrôles qualité : DataFrame valide + DataFrame rejeté.

    Retourné par ``QualityChecker.check()``. Le pipeline utilise :

    - ``valid`` : pour poursuivre le traitement (transform → SCD1/SCD2).
    - ``rejected`` : pour l'écriture dans ``rejects.rej_<table>``.
    - ``rejection_details`` : pour l'audit / le message de log.

    Attributes:
        valid: Lignes ayant passé tous les contrôles (colonne
            ``_rejection_reason`` retirée).
        rejected: Lignes rejetées, avec ``_rejection_reason`` conservée.
        rejection_details: Résumé textuel pour les logs (ex :
            ``["12 ligne(s) rejetée(s)"]``).

    """

    valid: DataFrame
    rejected: DataFrame
    rejection_details: list[str] = field(default_factory=list)


class QualityChecker:
    """Exécute les contrôles DQ techniques et sépare les lignes valides des rejets.

    Contrôles appliqués dans l'ordre :

    1. **Complétude** : champs obligatoires non nuls (``contract.completeness_columns``).
    2. **Validité de format** : REGEX ou DATE selon ``format_type`` du contract.
    3. **Doublons** : lignes entièrement identiques (garde la 1ʳᵉ occurrence).
    4. **Unicité** : clé fonctionnelle unique (garde la 1ʳᵉ occurrence).

    Les contrôles sont **cumulatifs** : une même ligne peut cumuler plusieurs
    raisons de rejet, concaténées avec ``|`` dans ``_rejection_reason``
    (ex : ``completeness:email|validity_regex:phone``).

    Exemple:
        >>> checker = QualityChecker()
        >>> result = checker.check(df_read, contract)
        >>> result.valid.count()      # → 998
        >>> result.rejected.count()   # → 2
    """

    # Nom de la colonne technique utilisée pour tracer les raisons de rejet.
    # Elle est retirée du DataFrame ``valid`` en fin de traitement, mais
    # conservée dans ``rejected`` pour être écrite dans la table de rejets.
    _REJECTION_COL = "_rejection_reason"

    def check(self, df: DataFrame, contract: DataContract) -> QualityResult:
        """Exécute tous les contrôles et retourne (valid, rejected).

        Séquence :

        1. Ajout de la colonne technique ``_rejection_reason`` (initialisée à null).
        2. Application successive des 4 contrôles — chacun ajoute une raison
           de rejet aux lignes qui violent la règle.
        3. Split : ``valid`` = lignes où ``_rejection_reason IS NULL``,
           ``rejected`` = lignes où ``_rejection_reason IS NOT NULL``.

        Args:
            df: DataFrame source à valider.
            contract: Contract défini dans ``contracts/<table>.yml`` — porte
                les colonnes obligatoires, les clés fonctionnelles, les
                règles de format.

        Returns:
            ``QualityResult(valid, rejected, rejection_details)``. Le pipeline
            écrit ``rejected`` dans ``rejects.rej_<table>`` via le
            ``RejectionHandler``, et poursuit avec ``valid``.

        Exemple:
            >>> # Contract avec ``completeness_columns = ["email"]``
            >>> result = checker.check(df, contract)
            >>> result.valid.count()      # → toutes les lignes avec email non nul
            >>> result.rejected.select("_rejection_reason").show()
            # +---------------------+
            # | _rejection_reason   |
            # +---------------------+
            # | completeness:email  |
            # +---------------------+

        """
        _log.info(
            "[QualityChecker] Début des contrôles DQ pour le contract '%s'",
            contract.table_name,
        )

        # Initialisation de la colonne technique de rejet à null.
        # Chaque contrôle va y ajouter une raison si la ligne est en violation.
        df = df.withColumn(self._REJECTION_COL, F.lit(None).cast("string"))

        # ----- Contrôle 1 : complétude -----
        # Vérifie que les colonnes déclarées obligatoires ne sont pas null.
        _log.debug("[QualityChecker] Contrôle 1 : complétude")
        df = self._check_completeness(df, contract)

        # ----- Contrôle 2 : validité de format (REGEX / DATE) -----
        # Vérifie les formats REGEX (emails, codes postaux…) et DATE.
        _log.debug("[QualityChecker] Contrôle 2 : validité de format")
        df = self._check_validity(df, contract)

        # ----- Contrôle 3 : doublons (lignes strictement identiques) -----
        # Deux lignes strictement identiques ⇒ on garde la première.
        _log.debug("[QualityChecker] Contrôle 3 : doublons de lignes")
        df = self._check_duplicates(df)

        # ----- Contrôle 4 : unicité de la clé fonctionnelle -----
        # Deux lignes avec la même clé mais des valeurs différentes ⇒ on
        # garde la première, on rejette les autres.
        _log.debug("[QualityChecker] Contrôle 4 : unicité de la clé fonctionnelle")
        df, dup_df = self._check_uniqueness(df, contract)

        # Split valid / rejected : rejetée dès qu'un contrôle a marqué la colonne.
        df_rejected = df.filter(F.col(self._REJECTION_COL).isNotNull())
        # On retire la colonne technique du DataFrame valid — elle ne doit
        # pas polluer la table cible.
        df_valid = df.filter(F.col(self._REJECTION_COL).isNull()).drop(
            self._REJECTION_COL
        )

        # Fusion doublons de clé + autres rejets (allowMissingColumns protège
        # contre l'évolution de schéma entre les deux DataFrames).
        if dup_df is not None:
            df_rejected = df_rejected.unionByName(dup_df, allowMissingColumns=True)

        details = []
        # ``count()`` force l'exécution ⇒ coûteux mais nécessaire pour le log.
        rej_count = df_rejected.count()
        if rej_count > 0:
            details.append(f"{rej_count} ligne(s) rejetée(s)")
            _log.warning(
                "[QualityChecker] %d ligne(s) rejetée(s) pour '%s'",
                rej_count,
                contract.table_name,
            )
        else:
            _log.info("[QualityChecker] Aucun rejet pour '%s'", contract.table_name)

        return QualityResult(
            valid=df_valid, rejected=df_rejected, rejection_details=details
        )

    def _check_completeness(self, df: DataFrame, contract: DataContract) -> DataFrame:
        """Marque les lignes avec des champs obligatoires nuls.

        Une ligne violant la complétude reçoit la raison
        ``completeness:<column_name>`` dans ``_rejection_reason``.

        Exemple:
            ``contract.completeness_columns = ["email", "phone"]``
            - email=null              ⇒ raison = ``completeness:email``
            - email=null AND phone=null
              ⇒ raison = ``completeness:email|completeness:phone``
        """
        # On ne contrôle que les colonnes réellement présentes — protège
        # contre les décalages entre le contract et la source.
        cols_to_check = [c for c in contract.completeness_columns if c in df.columns]
        _log.debug(
            "[QualityChecker] Complétude — %d colonne(s) contrôlée(s) : %s",
            len(cols_to_check),
            cols_to_check,
        )
        for col_name in cols_to_check:
            # Condition = colonne null ⇒ on marque la ligne.
            condition = F.col(col_name).isNull()
            reason = f"completeness:{col_name}"
            df = self._add_rejection_reason(df, condition, reason)
        return df

    def _check_validity(self, df: DataFrame, contract: DataContract) -> DataFrame:
        """Marque les lignes avec des formats invalides (REGEX ou DATE).

        Deux types de contrôles :

        - ``REGEX`` : la valeur doit matcher un motif (email, code postal…).
        - ``DATE``  : la valeur doit être parsable au format déclaré
          (ex : ``yyyy-MM-dd``).

        Les valeurs **nulles** sont ignorées ici — la complétude s'en occupe
        si nécessaire.

        Exemple:
            colonne "email", REGEX ``^[^@]+@[^@]+$``
            - "toto"        ⇒ raison = ``validity_regex:email``
            - "a@b.com"     ⇒ aucune raison
            - null          ⇒ aucune raison (géré par completeness)
        """
        _log.debug(
            "[QualityChecker] Validité — %d colonne(s) avec contrainte de format",
            len(contract.validity_columns),
        )
        for col_contract in contract.validity_columns:
            col_name = col_contract.name
            if col_name not in df.columns:
                continue

            if col_contract.format_type == "REGEX" and col_contract.format_value:
                # Contrôle regex — uniquement sur les valeurs non nulles
                condition = F.col(col_name).isNotNull() & ~F.col(col_name).cast(
                    "string"
                ).rlike(col_contract.format_value)
                reason = f"validity_regex:{col_name}"
                df = self._add_rejection_reason(df, condition, reason)

            elif col_contract.format_type == "DATE" and col_contract.format_value:
                # Contrôle de format de date — la conversion doit réussir
                condition = (
                    F.col(col_name).isNotNull()
                    & F.to_date(
                        F.col(col_name).cast("string"), col_contract.format_value
                    ).isNull()
                )
                reason = f"validity_date:{col_name}"
                df = self._add_rejection_reason(df, condition, reason)

        return df

    def _check_duplicates(self, df: DataFrame) -> DataFrame:
        """Marque les lignes strictement identiques (hors ``_rejection_reason``).

        Approche :

        1. Compter les occurrences de chaque combinaison de valeurs.
        2. Numéroter les lignes de chaque groupe (``row_number`` + Window).
        3. Marquer comme doublons les lignes n°2, 3, … du groupe.

        On conserve la 1ʳᵉ occurrence pour éviter de perdre la donnée.

        Exemple:
            Input : 3 lignes identiques (id=1, name="A")
            - ligne 1 : conservée
            - lignes 2 et 3 : raison = ``duplicate_row``
        """
        _log.debug("[QualityChecker] Détection des lignes entièrement dupliquées")
        # Comparer sur toutes les colonnes SAUF la colonne technique de rejet
        # (sinon deux lignes rejetées différemment seraient vues comme distinctes).
        data_cols = [c for c in df.columns if c != self._REJECTION_COL]
        # Compter les lignes identiques via une window sur toutes les colonnes.
        window = F.count("*").over(Window.partitionBy(*data_cols))
        df = df.withColumn("_dup_count", window)
        # ``monotonically_increasing_id`` = ordre stable pour désigner la 1ʳᵉ
        # occurrence (indispensable pour un ``row_number`` déterministe).
        df = df.withColumn("_row_id", F.monotonically_increasing_id())
        w = Window.partitionBy(data_cols).orderBy("_row_id")
        df = df.withColumn("_row_num", F.row_number().over(w))
        # Rejet ⇔ groupe > 1 ET pas la 1ʳᵉ occurrence.
        condition = (F.col("_dup_count") > 1) & (F.col("_row_num") > 1)
        df = self._add_rejection_reason(df, condition, "duplicate_row")
        # Nettoyage des colonnes techniques temporaires.
        df = df.drop("_dup_count", "_row_id", "_row_num")
        return df

    def _check_uniqueness(
        self,
        df: DataFrame,
        contract: DataContract,
    ) -> tuple[DataFrame, DataFrame | None]:
        """Vérifie l'unicité de la clé fonctionnelle — rejette les doublons de clé.

        Différent de ``_check_duplicates`` : ici deux lignes peuvent avoir
        des valeurs différentes mais la même clé fonctionnelle (ex : deux
        salaires différents pour le même ``employee_id``). On conserve la 1ʳᵉ
        occurrence et on rejette les suivantes.

        Exemple:
            ``functional_keys = ["employee_id"]``
            - 2 lignes avec ``employee_id=42``, ``name`` différent
              → ligne 1 : conservée
              → ligne 2 : raison = ``uniqueness_key:employee_id``
        """
        keys = contract.functional_keys
        if not keys:
            _log.debug(
                "[QualityChecker] Aucune clé fonctionnelle — contrôle d'unicité ignoré"
            )
            return df, None

        # Filtrer les colonnes clé effectivement présentes dans le DataFrame
        existing_keys = [k for k in keys if k in df.columns]
        if not existing_keys:
            return df, None

        _log.debug(
            "[QualityChecker] Contrôle d'unicité sur les clés : %s", existing_keys
        )
        w = Window.partitionBy(existing_keys).orderBy(F.monotonically_increasing_id())
        df = df.withColumn("_key_row_num", F.row_number().over(w))

        # Les doublons de clé = numéro de ligne > 1 pour la même combinaison de clés
        condition = F.col("_key_row_num") > 1
        df = self._add_rejection_reason(
            df, condition, f"uniqueness_key:{','.join(existing_keys)}"
        )
        df = df.drop("_key_row_num")

        return df, None  # Les rejets sont déjà dans _rejection_reason

    def _add_rejection_reason(
        self,
        df: DataFrame,
        condition: F.Column,
        reason: str,
    ) -> DataFrame:
        """Ajoute une raison de rejet à ``_rejection_reason`` (concaténation).

        Logique :

        - Si condition non remplie : on garde la valeur existante.
        - Sinon, si la ligne a déjà une raison : on concatène avec ``|``.
        - Sinon : on met la nouvelle raison.

        Cela permet d'accumuler plusieurs raisons sur une même ligne.

        Exemple:
            Ligne 1 : raison existante = null, condition = True
              → nouvelle raison = ``completeness:email``
            Ligne 2 : raison existante = ``completeness:email``, condition = True
              → nouvelle raison = ``completeness:email|validity_regex:phone``
        """
        return df.withColumn(
            self._REJECTION_COL,
            F.when(
                condition,
                # Condition remplie ⇒ ajouter / concaténer la raison.
                F.when(
                    F.col(self._REJECTION_COL).isNotNull(),
                    F.concat(F.col(self._REJECTION_COL), F.lit("|"), F.lit(reason)),
                ).otherwise(F.lit(reason)),
                # Condition non remplie ⇒ garder la valeur actuelle (peut être null).
            ).otherwise(F.col(self._REJECTION_COL)),
        )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
