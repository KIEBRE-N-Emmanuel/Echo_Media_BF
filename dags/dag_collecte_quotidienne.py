"""
dag_collecte_quotidienne
========================
Réf. cahier des charges : §2.1, §6.4, EF-01, EF-02, EF-03.

Collecte quotidienne (traitement par lot, pas de temps réel) sur les sources
qualifiées : RSS, GDELT, NewsAPI/GNews, et scraping en complément. Écrit les
contenus bruts filtrés (mention explicite du Burkina Faso) dans raw_articles.

Déclenchement : cron quotidien à 05h00 (heure d'Ouagadougou = UTC).
Ce DAG "publie" le Dataset RAW_ARTICLES_DATASET, qui déclenche automatiquement
dag_nettoyage_dedup dès que la collecte du jour est terminée (chaînage par
Datasets plutôt que par TriggerDagRunOperator — plus robuste, cf. discussion
sur le chaînage inter-DAG).
"""
from __future__ import annotations

from datetime import datetime, timedelta

import httpx
import feedparser
from tenacity import retry, stop_after_attempt, wait_exponential

from airflow.datasets import Dataset
from airflow.decorators import dag, task, task_group

from common import config, db

RAW_ARTICLES_DATASET = Dataset("postgres://echomedia_db/raw_articles")

default_args = {
    "owner": "equipe_data",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": db.task_failure_alert,
    "execution_timeout": timedelta(hours=1),
}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=30))
def _fetch(url: str, params: dict | None = None) -> httpx.Response:
    with httpx.Client(timeout=20, follow_redirects=True) as client:
        resp = client.get(url, params=params, headers={"User-Agent": "EchoMediaBF/1.0"})
        resp.raise_for_status()
        return resp


def _est_pertinent(texte: str, langue: str) -> bool:
    """Filtrage de pertinence EF-03 : mention explicite du Burkina Faso."""
    if not texte:
        return False
    mots_cles = config.get_relevance_keywords(langue) or config.get_relevance_keywords("fr")
    texte_lower = texte.lower()
    return any(mc.lower() in texte_lower for mc in mots_cles)


def _insert_raw(rows: list[dict], run_id: str) -> int:
    if not rows:
        return 0
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO raw_articles
                        (source_id, source_type, url, titre_brut, texte_brut, date_publication, run_id)
                    VALUES (%(source_id)s, %(source_type)s, %(url)s, %(titre_brut)s,
                            %(texte_brut)s, %(date_publication)s, %(run_id)s)
                    ON CONFLICT (url) DO NOTHING
                    """,
                    {**row, "run_id": run_id},
                )
    return len(rows)


@dag(
    dag_id="dag_collecte_quotidienne",
    description="Collecte quotidienne multi-sources mentionnant le Burkina Faso",
    schedule="0 5 * * *",          # tous les jours à 05h00
    start_date=datetime(2026, 8, 25),   # date de mise en route du pipeline (§2.2 : pas de rattrapage rétroactif)
    catchup=False,                  # pas de rattrapage d'historique — collecte à partir de la mise en route
    max_active_runs=1,
    default_args=default_args,
    tags=["echomedia", "collecte"],
)
def dag_collecte_quotidienne():

    @task_group(group_id="collecte_rss")
    def collecte_rss():
        @task
        def fetch_flux_rss(source: dict, run_id: str = "{{ run_id }}") -> int:
            resp = _fetch(source["url"])
            parsed = feedparser.parse(resp.text)
            rows = []
            for entry in parsed.entries:
                titre = entry.get("title", "")
                texte = entry.get("summary", "") or entry.get("description", "")
                if not _est_pertinent(f"{titre} {texte}", source["langue"]):
                    continue
                rows.append(
                    {
                        "source_id": source["id"],
                        "source_type": "rss",
                        "url": entry.get("link"),
                        "titre_brut": titre,
                        "texte_brut": texte,
                        "date_publication": entry.get("published", None),
                    }
                )
            return _insert_raw(rows, run_id)

        # Un mapping dynamique : une task par source RSS active, en parallèle
        fetch_flux_rss.expand(source=config.get_sources_by_type("rss"))

    @task_group(group_id="collecte_api")
    def collecte_api():
        @task
        def fetch_gdelt(run_id: str = "{{ run_id }}") -> int:
            """API GDELT DOC 2.0 — utilisée comme baseline de ton (§7)."""
            source = config.get_sources_by_type("api_gdelt")[0]
            resp = _fetch(
                "https://api.gdeltproject.org/api/v2/doc/doc",
                params={
                    "query": source["query"],
                    "mode": "artlist",
                    "format": "json",
                    "maxrecords": 250,
                    "timespan": "1d",
                },
            )
            data = resp.json()
            rows = [
                {
                    "source_id": "gdelt",
                    "source_type": "api_gdelt",
                    "url": a.get("url"),
                    "titre_brut": a.get("title"),
                    "texte_brut": None,   # GDELT ne fournit pas le corps -> scraping en aval si retenu
                    "date_publication": a.get("seendate"),
                }
                for a in data.get("articles", [])
            ]
            return _insert_raw(rows, run_id)

        @task
        def fetch_newsapi(run_id: str = "{{ run_id }}") -> int:
            import os

            api_key = os.environ.get("NEWSAPI_KEY")
            if not api_key:
                raise ValueError("NEWSAPI_KEY manquant (secret non configuré)")
            source = config.get_sources_by_type("api_newsapi")[0]
            resp = _fetch(
                "https://newsapi.org/v2/everything",
                params={"q": source["query"], "language": "fr", "pageSize": 100, "apiKey": api_key},
            )
            data = resp.json()
            rows = []
            for a in data.get("articles", []):
                texte = f"{a.get('title', '')} {a.get('description', '')}"
                if not _est_pertinent(texte, "fr"):
                    continue
                rows.append(
                    {
                        "source_id": "newsapi",
                        "source_type": "api_newsapi",
                        "url": a.get("url"),
                        "titre_brut": a.get("title"),
                        "texte_brut": a.get("content") or a.get("description"),
                        "date_publication": a.get("publishedAt"),
                    }
                )
            return _insert_raw(rows, run_id)

        fetch_gdelt()
        fetch_newsapi()

    @task_group(group_id="collecte_scraping")
    def collecte_scraping():
        @task
        def scrape_source(source: dict, run_id: str = "{{ run_id }}") -> int:
            """Scraping HTML en complément uniquement (§2.3), via trafilatura."""
            import trafilatura

            resp = _fetch(source["url"])
            downloaded = trafilatura.extract(resp.text, output_format="json", with_metadata=True)
            if not downloaded:
                return 0
            import json as _json

            meta = _json.loads(downloaded)
            texte = meta.get("text", "")
            if not _est_pertinent(texte, source["langue"]):
                return 0
            rows = [
                {
                    "source_id": source["id"],
                    "source_type": "scraping",
                    "url": source["url"],
                    "titre_brut": meta.get("title"),
                    "texte_brut": texte,
                    "date_publication": meta.get("date"),
                }
            ]
            return _insert_raw(rows, run_id)

        scrape_source.expand(source=config.get_sources_by_type("scraping"))

    @task(outlets=[RAW_ARTICLES_DATASET])
    def marquer_collecte_terminee(run_id: str = "{{ run_id }}") -> None:
        """Task de clôture : ne fait rien de métier, mais publie le Dataset
        qui déclenche dag_nettoyage_dedup une fois les 3 branches terminées."""
        db.log_run("dag_collecte_quotidienne", "marquer_collecte_terminee", run_id, "success")

    [collecte_rss(), collecte_api(), collecte_scraping()] >> marquer_collecte_terminee()


dag_collecte_quotidienne()
