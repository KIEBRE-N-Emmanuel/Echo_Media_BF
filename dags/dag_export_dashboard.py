"""
dag_export_dashboard
=====================
Réf. cahier des charges : §6.1 (étapes 8-9), EF-16, EF-17, EF-18.

Déclenché par la publication du Dataset SCORED_ARTICLES_DATASET. Reconstruit
la table dénormalisée dashboard_articles (servie au tableau de bord) et
génère les exports CSV/Excel documentés.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.datasets import Dataset
from airflow.decorators import dag, task

from common import db

SCORED_ARTICLES_DATASET = Dataset("postgres://echomedia_db/scored_articles")

default_args = {
    "owner": "equipe_data",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": db.task_failure_alert,
    "execution_timeout": timedelta(minutes=30),
}

EXPORT_DIR = Path("/opt/airflow/exports")


@dag(
    dag_id="dag_export_dashboard",
    description="Rafraîchit la table servie au dashboard et génère les exports",
    schedule=[SCORED_ARTICLES_DATASET],
    start_date=datetime(2026, 8, 25),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["echomedia", "export"],
)
def dag_export_dashboard():

    @task
    def rafraichir_dashboard_articles(run_id: str = "{{ run_id }}") -> int:
        """Upsert dénormalisé : jointure raw -> clean -> scored, une seule
        fois ici plutôt qu'à chaque requête du dashboard (EF-17)."""
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dashboard_articles
                        (id, source_id, source_nom, titre, url, date_publication, langue,
                         nature_contenu, themes, orientation, score_continu, ton, resume_cible)
                    SELECT
                        s.id, r.source_id, r.source_id, r.titre_brut, r.url, r.date_publication,
                        c.langue, s.nature_contenu, s.themes, s.orientation, s.score_continu,
                        s.ton, s.resume_cible
                    FROM scored_articles s
                    JOIN clean_articles c ON c.id = s.clean_article_id
                    JOIN raw_articles r ON r.id = c.raw_article_id
                    ON CONFLICT (id) DO UPDATE SET
                        orientation = EXCLUDED.orientation,
                        score_continu = EXCLUDED.score_continu,
                        ton = EXCLUDED.ton,
                        resume_cible = EXCLUDED.resume_cible,
                        maj_le = now()
                    """
                )
                nb = cur.rowcount
        db.log_run("dag_export_dashboard", "rafraichir_dashboard_articles", run_id, "success", {"n": nb})
        return nb

    @task
    def exporter_csv_excel(run_id: str = "{{ run_id }}") -> str:
        """EF-18 : export CSV/Excel des résultats, documenté (voir README
        pour le format des colonnes)."""
        import pandas as pd

        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        with db.get_conn() as conn:
            df = pd.read_sql("SELECT * FROM dashboard_articles ORDER BY date_publication DESC", conn)

        horodatage = datetime.utcnow().strftime("%Y%m%d_%H%M")
        csv_path = EXPORT_DIR / f"echomedia_export_{horodatage}.csv"
        xlsx_path = EXPORT_DIR / f"echomedia_export_{horodatage}.xlsx"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        df.to_excel(xlsx_path, index=False)

        db.log_run(
            "dag_export_dashboard", "exporter_csv_excel", run_id, "success",
            {"lignes": len(df), "csv": str(csv_path), "xlsx": str(xlsx_path)},
        )
        return str(xlsx_path)

    rafraichir_dashboard_articles() >> exporter_csv_excel()


dag_export_dashboard()
