"""
Chargement centralisé de la configuration du pipeline EchoMedia BF.

Principe (exigence de maintenabilité, §5) : les sources, thèmes et seuils de
scoring sont dans des fichiers YAML versionnés sous /config, jamais en dur
dans le code des DAGs. Un analyste peut modifier ces fichiers sans redéployer
de code Python.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import yaml

CONFIG_DIR = Path(os.environ.get("ECHOMEDIA_CONFIG_DIR", "/opt/airflow/config"))


@lru_cache(maxsize=None)
def load_sources() -> list[dict]:
    with open(CONFIG_DIR / "sources.yaml", "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return [s for s in data["sources"] if s.get("actif", True)]


@lru_cache(maxsize=None)
def load_scoring_config() -> dict:
    with open(CONFIG_DIR / "scoring.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_sources_by_type(source_type: str) -> list[dict]:
    return [s for s in load_sources() if s["type"] == source_type]


def get_relevance_keywords(langue: str) -> list[str]:
    return load_scoring_config()["relevance_keywords"].get(langue, [])


def get_themes() -> list[str]:
    return load_scoring_config()["themes"]


def get_opinion_scale() -> dict:
    return load_scoring_config()["opinion_scale"]
