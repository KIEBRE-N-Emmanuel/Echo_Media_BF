"""
dag_classification_scoring
===========================
Réf. cahier des charges : §6.1 (étapes 4-7), §2.6, EF-06 à EF-14.

Déclenché par la publication du Dataset CLEAN_ARTICLES_DATASET. Pour chaque
article nettoyé : identifie le passage relatif au Burkina Faso, produit un
résumé ciblé, classe par nature/thème, calcule le score d'opinion (orientation,
score continu, ton, confiance) et extrait les entités nommées (Should).

Le modèle utilisé en M1 est une API LLM externe (cf. §6.2 : "Modèle zero-shot
multilingue ou API LLM en M1, calibré sur échantillon annoté"). Le prompt et
le format de sortie sont volontairement stricts (JSON) pour que le scoring
reste vérifiable manuellement (EF-13).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task

from common import config, db

CLEAN_ARTICLES_DATASET = Dataset("postgres://echomedia_db/clean_articles")
SCORED_ARTICLES_DATASET = Dataset("postgres://echomedia_db/scored_articles")

default_args = {
    "owner": "equipe_data",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": db.task_failure_alert,
    "execution_timeout": timedelta(hours=2),   # étape la plus coûteuse du pipeline (appels API LLM)
}

MODELE_UTILISE = "claude-sonnet-4-6"   # tracé dans scored_articles.modele_utilise (EF-13)

PROMPT_TEMPLATE = """Tu analyses un article de presse pour un projet de veille média sur le Burkina Faso.
Thèmes autorisés (choisis-en un ou plusieurs) : {themes}
Tons autorisés : {tons}

Article (titre : {titre}) :
\"\"\"{texte}\"\"\"

Réponds UNIQUEMENT en JSON valide, sans texte autour, avec ce format exact :
{{
  "passage_pertinent": "extrait du texte qui mentionne le Burkina Faso",
  "resume_cible": "résumé en 2-3 phrases de ce que dit l'article SPÉCIFIQUEMENT sur le Burkina Faso",
  "nature_contenu": "article_presse|depeche_agence|editorial_tribune|interview",
  "themes": ["theme1", "theme2"],
  "orientation": "favorable|neutre|defavorable",
  "score_continu": 0.0,
  "ton": "un des tons autorisés",
  "confiance_modele": 0.0,
  "entites_personnes": [],
  "entites_lieux": []
}}
"""


@dag(
    dag_id="dag_classification_scoring",
    description="Résumé ciblé, classification thématique et scoring d'opinion",
    schedule=[CLEAN_ARTICLES_DATASET],
    start_date=datetime(2026, 8, 25),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["echomedia", "scoring"],
)
def dag_classification_scoring():

    @task
    def recuperer_articles_a_scorer() -> list[dict]:
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.id, c.titre, c.texte_propre, c.langue
                    FROM clean_articles c
                    LEFT JOIN scored_articles s ON s.clean_article_id = c.id
                    WHERE c.est_doublon = false AND s.id IS NULL
                    """
                )
                cols = [d.name for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]

    @task
    def analyser_article(article: dict) -> dict:
        """Appel LLM pour un article : identification du passage pertinent,
        résumé ciblé, classification et scoring en une seule passe.
        Mappée dynamiquement (.expand) : une task par article, avec retries
        indépendants -- un échec sur un article ne bloque pas les autres."""
        import anthropic

        client = anthropic.Anthropic(api_key=os.environ["LLM_API_KEY"])
        scoring_cfg = config.load_scoring_config()

        prompt = PROMPT_TEMPLATE.format(
            themes=", ".join(scoring_cfg["themes"]),
            tons=", ".join(scoring_cfg["tons"]),
            titre=article.get("titre", ""),
            texte=article["texte_propre"][:8000],   # borne la taille envoyée
        )

        response = client.messages.create(
            model=MODELE_UTILISE,
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_text = response.content[0].text.strip()
        raw_text = raw_text.removeprefix("```json").removesuffix("```").strip()

        try:
            resultat = json.loads(raw_text)
        except json.JSONDecodeError:
            # Trace l'échec de parsing mais ne fait pas tomber tout le DAG :
            # l'article reste non scoré et sera repris au run suivant.
            db.log_run(
                "dag_classification_scoring", "analyser_article", "{{ run_id }}",
                "failed", {"clean_article_id": article["id"], "raw_response": raw_text[:500]},
            )
            return {}

        # Recalage de l'orientation à partir du score continu et des seuils
        # configurés (§11.1), pour rester cohérent même si le modèle hésite.
        echelle = config.get_opinion_scale()
        score = float(resultat.get("score_continu", 0))
        if score >= echelle["favorable"]["seuil_min"]:
            resultat["orientation"] = "favorable"
        elif score <= echelle["defavorable"]["seuil_max"]:
            resultat["orientation"] = "defavorable"
        else:
            resultat["orientation"] = "neutre"

        resultat["clean_article_id"] = article["id"]
        return resultat

    @task(outlets=[SCORED_ARTICLES_DATASET])
    def sauvegarder_scores(resultats: list[dict], run_id: str = "{{ run_id }}") -> int:
        resultats_valides = [r for r in resultats if r]
        if not resultats_valides:
            return 0
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                for r in resultats_valides:
                    cur.execute(
                        """
                        INSERT INTO scored_articles
                            (clean_article_id, passage_pertinent, resume_cible, nature_contenu,
                             themes, orientation, score_continu, ton, confiance_modele,
                             entites_personnes, entites_lieux, modele_utilise, run_id)
                        VALUES (%(clean_article_id)s, %(passage_pertinent)s, %(resume_cible)s,
                                %(nature_contenu)s, %(themes)s, %(orientation)s, %(score_continu)s,
                                %(ton)s, %(confiance_modele)s, %(entites_personnes)s,
                                %(entites_lieux)s, %(modele)s, %(run_id)s)
                        """,
                        {**r, "modele": MODELE_UTILISE, "run_id": run_id},
                    )
        db.log_run("dag_classification_scoring", "sauvegarder_scores", run_id, "success", {"n": len(resultats_valides)})
        return len(resultats_valides)

    articles = recuperer_articles_a_scorer()
    resultats = analyser_article.expand(article=articles)
    sauvegarder_scores(resultats)


dag_classification_scoring()
