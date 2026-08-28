#!/usr/bin/env python3
"""
Vérifie la validité technique de chaque source de config/sources.yaml :
- Le flux RSS répond (HTTP 200) et contient bien des entrées parsables
- Les pages de scraping répondent et contiennent du texte extractible

Usage :
    python3 scripts/verify_sources.py
    python3 scripts/verify_sources.py --update    # met aussi à jour verifie: true/false dans le fichier

Ne dépend pas d'Airflow — se lance en dehors des conteneurs, avec juste :
    pip install httpx feedparser trafilatura pyyaml --break-system-packages
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import httpx
import feedparser
import yaml

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "sources.yaml"
TIMEOUT = 15
HEADERS = {"User-Agent": "EchoMediaBF-SourceCheck/1.0"}


def verifier_rss(url: str) -> tuple[bool, str]:
    try:
        resp = httpx.get(url, headers=HEADERS, timeout=TIMEOUT, follow_redirects=True)
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}"
        parsed = feedparser.parse(resp.text)
        if not parsed.entries:
            return False, "réponse 200 mais aucune entrée RSS détectée (mauvaise URL ou format inattendu)"
        return True, f"OK — {len(parsed.entries)} entrées détectées"
    except Exception as e:
        return False, f"erreur : {e}"


def verifier_scraping(url: str) -> tuple[bool, str]:
    try:
        resp = httpx.get(url, headers=HEADERS, timeout=TIMEOUT, follow_redirects=True)
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}"
        if len(resp.text) < 500:
            return False, "page trop courte, probablement bloquée ou vide"
        return True, f"OK — page accessible ({len(resp.text)} caractères)"
    except Exception as e:
        return False, f"erreur : {e}"


def verifier_api(source: dict) -> tuple[bool, str]:
    # Les API (GDELT, NewsAPI) nécessitent des clés ou une syntaxe différente,
    # non testées ici automatiquement — vérification manuelle recommandée.
    return None, "vérification API non automatisée (nécessite une clé) — à tester via un run réel du DAG"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--update", action="store_true", help="Met à jour verifie: true/false dans le fichier")
    args = parser.parse_args()

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    resultats = []
    for source in data["sources"]:
        stype = source["type"]
        if stype == "rss":
            ok, detail = verifier_rss(source["url"])
        elif stype == "scraping":
            ok, detail = verifier_scraping(source["url"])
        else:
            ok, detail = verifier_api(source)

        resultats.append((source["id"], stype, ok, detail))
        statut = "✅" if ok else ("⚠️ " if ok is None else "❌")
        print(f"{statut} {source['id']:20s} [{stype:10s}] {detail}")

        if args.update and ok is not None:
            source["verifie"] = ok

    if args.update:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            yaml.dump(data, f, allow_unicode=True, sort_keys=False, width=100)
        print(f"\n→ {CONFIG_PATH} mis à jour (champ 'verifie').")

    echecs = [r for r in resultats if r[2] is False]
    if echecs:
        print(f"\n{len(echecs)} source(s) en échec à corriger avant le premier run réel.")
        sys.exit(1)


if __name__ == "__main__":
    main()
