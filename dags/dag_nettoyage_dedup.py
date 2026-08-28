"""
dag_nettoyage_dedup
====================
Réf. cahier des charges : §6.1 (étapes 2-3), EF-04, EF-05.

Déclenché automatiquement dès que dag_collecte_quotidienne publie le Dataset
RAW_ARTICLES_DATASET (pas de cron ici : schedule = liste de Datasets).
Nettoie le texte, détecte la langue (filtre FR/EN), déduplique par hash de
contenu et similarité de titre, écrit dans clean_articles.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from difflib import SequenceMatcher

from airflow.datasets import Dataset
from airflow.decorators import dag, task

from common import config, db

RAW_ARTICLES_DATASET = Dataset("postgres://echomedia_db/raw_articles")
CLEAN_ARTICLES_DATASET = Dataset("postgres://echomedia_db/clean_articles")

default_args = {
    "owner": "equipe_data",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": db.task_failure_alert,
    "execution_timeout": timedelta(minutes=45),
}


def _hash_contenu(texte: str) -> str:
    return hashlib.sha256(texte.strip().lower().encode("utf-8")).hexdigest()


def _titres_similaires(titre_a: str, titre_b: str, seuil: float = 0.9) -> bool:
    if not titre_a or not titre_b:
        return False
    return SequenceMatcher(None, titre_a.lower(), titre_b.lower()).ratio() >= seuil


@dag(
    dag_id="dag_nettoyage_dedup",
    description="Nettoyage, détection de langue et déduplication des contenus collectés",
    schedule=[RAW_ARTICLES_DATASET],   # déclenché par la fin de la collecte du jour
    start_date=datetime(2026, 8, 25),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["echomedia", "nettoyage"],
)
def dag_nettoyage_dedup():

    @task
    def recuperer_articles_non_traites(run_id: str = "{{ run_id }}") -> list[dict]:
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT r.id, r.titre_brut, r.texte_brut, r.source_id
                    FROM raw_articles r
                    LEFT JOIN clean_articles c ON c.raw_article_id = r.id
                    WHERE c.id IS NULL
                    """
                )
                cols = [d.name for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]

    @task
    def extraire_texte_propre(articles: list[dict]) -> list[dict]:
        import trafilatura

        resultats = []
        for a in articles:
            texte = a.get("texte_brut") or ""
            texte_propre = trafilatura.extract(texte) if "<" in texte else texte
            resultats.append({**a, "texte_propre": (texte_propre or texte).strip()})
        return resultats

    @task
    def detecter_langue(articles: list[dict]) -> list[dict]:
        """Filtre FR/EN — Should on utilise fastText en prod ; heuristique
        simple ici en fallback si le modèle n'est pas chargé."""
        try:
            import fasttext

            modele = fasttext.load_model("/opt/airflow/models/lid.176.ftz")

            def detect(texte: str) -> str:
                pred = modele.predict(texte.replace("\n", " ")[:500])
                return pred[0][0].replace("__label__", "")

        except Exception:
            mots_fr = {"le", "la", "les", "des", "est", "une", "dans"}

            def detect(texte: str) -> str:
                mots = set(texte.lower().split()[:50])
                return "fr" if mots & mots_fr else "en"

        resultats = []
        for a in articles:
            langue = detect(a["texte_propre"])
            if langue not in ("fr", "en"):
                continue  # hors périmètre linguistique M1 (§2.2)
            resultats.append({**a, "langue": langue})
        return resultats

    @task
    def dedupliquer(articles: list[dict]) -> list[dict]:
        """EF-05 : hash de contenu + similarité de titre. Le hash gère les
        republications identiques ; la similarité gère les quasi-doublons
        (même dépêche reprise avec un titre légèrement modifié)."""
        vus_hash: set[str] = set()
        vus_titres: list[str] = []
        resultats = []
        for a in articles:
            h = _hash_contenu(a["texte_propre"])
            if h in vus_hash:
                continue
            if any(_titres_similaires(a.get("titre_brut", ""), t) for t in vus_titres):
                continue
            vus_hash.add(h)
            vus_titres.append(a.get("titre_brut", ""))
            resultats.append({**a, "hash_contenu": h})
        return resultats

    @task(outlets=[CLEAN_ARTICLES_DATASET])
    def sauvegarder_clean(articles: list[dict], run_id: str = "{{ run_id }}") -> int:
        if not articles:
            return 0
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                for a in articles:
                    cur.execute(
                        """
                        INSERT INTO clean_articles
                            (raw_article_id, titre, texte_propre, langue, hash_contenu, run_id)
                        VALUES (%(id)s, %(titre_brut)s, %(texte_propre)s, %(langue)s, %(hash_contenu)s, %(run_id)s)
                        ON CONFLICT (hash_contenu) DO NOTHING
                        """,
                        {**a, "run_id": run_id},
                    )
        db.log_run("dag_nettoyage_dedup", "sauvegarder_clean", run_id, "success", {"n": len(articles)})
        return len(articles)

    articles_bruts = recuperer_articles_non_traites()
    texte_propre = extraire_texte_propre(articles_bruts)
    avec_langue = detecter_langue(texte_propre)
    sans_doublons = dedupliquer(avec_langue)
    sauvegarder_clean(sans_doublons)


dag_nettoyage_dedup()
