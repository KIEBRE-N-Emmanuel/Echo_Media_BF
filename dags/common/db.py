"""
Accès à la base de données métier (echomedia_db, distincte de la base interne
d'Airflow) et journalisation des exécutions pour la traçabilité (§5).
"""
from __future__ import annotations

import json
from contextlib import contextmanager

from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "echomedia_db"


def get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONN_ID)


@contextmanager
def get_conn():
    hook = get_hook()
    conn = hook.get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def log_run(dag_id: str, task_id: str, run_id: str, statut: str, details: dict | None = None) -> None:
    """Écrit une ligne dans pipeline_run_log — appelé depuis chaque task
    (succès) et depuis on_failure_callback (échec)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_run_log (dag_id, task_id, run_id, statut, details)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (dag_id, task_id, run_id, statut, json.dumps(details or {})),
            )


def task_failure_alert(context) -> None:
    """on_failure_callback commun à tous les DAGs : journalise l'échec.
    À brancher plus tard sur un canal de notif (Slack/e-mail) si besoin."""
    ti = context["task_instance"]
    log_run(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=context["run_id"],
        statut="failed",
        details={"exception": str(context.get("exception"))},
    )
