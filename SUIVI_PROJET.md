# Suivi du projet — EchoMedia BF

Dernière mise à jour : 27 août 2026

Ce fichier reflète l'avancement réel par rapport aux livrables du §9 et aux
critères de succès du §10 du cahier des charges. Légende : ✅ Fait · 🔄 En
cours · ⬜ À faire.

## Vue d'ensemble par semaine (planning §8)

| Semaine | Objectif | Statut |
|---|---|---|
| S1 | Cadrage : sources, langues, thèmes, scoring | 🔄 En cours |
| S2 | Airflow + PostgreSQL, premiers connecteurs | ✅ Structure faite, 🔄 données réelles à venir |
| S3 | Classification/scoring calibrés, corpus annoté | ⬜ À faire |
| S4 | Automatisation complète, dashboard, tests, doc | ⬜ À faire |

## Détail par chantier

### 1. Sources médiatiques (§2.3)
- ✅ Structure du fichier de config (`config/sources.yaml`), 18 sources, national + international
- ✅ 4 sources nationales vérifiées manuellement (flux RSS testés par recherche) : Sidwaya, Burkina24, AIB, LeFaso.net
- ✅ Script `scripts/verify_sources.py` prêt — teste automatiquement chaque source (HTTP + parsing RSS)
- 🔄 **14 sources restantes à vérifier avec le script, depuis votre machine** (mon environnement d'exécution n'a pas d'accès internet général, donc je ne peux pas les tester moi-même — voir commande ci-dessous)
- ⬜ Décision finale sur l'inclusion des réseaux sociaux (X/Facebook/TikTok) — coût API et conformité CGU à trancher avec BF Society
- ⬜ Validation finale de la liste par le référent projet

**Commande à lancer chez vous** (hors Docker, juste avec Python) :
```bash
pip install httpx feedparser pyyaml --break-system-packages
python3 scripts/verify_sources.py --update
```
Le `--update` corrige automatiquement le champ `verifie:` dans `sources.yaml` selon le résultat réel.

### 2. Pipeline Airflow (§6.4)
- ✅ 4 DAGs codés et chaînés par Datasets (`dags/`)
- ✅ Schéma de base de données (`sql/schema.sql`)
- ✅ Environnement Docker prêt à démarrer (`docker-compose.yml`)
- ⬜ Premier run réel sur les sources vérifiées (pas encore lancé en conditions réelles)
- ⬜ Téléchargement du modèle fastText (`lid.176.ftz`), actuellement fallback approximatif
- ⬜ Vérification des taux de réussite des exécutions planifiées (critère §10)

### 3. Scoring d'opinion (§2.6, §11.1)
- ✅ Échelle définie dans `config/scoring.yaml` (seuils indicatifs)
- ✅ Prompt LLM de départ dans `dag_classification_scoring.py`
- ⬜ Constitution d'un échantillon annoté manuellement
- ⬜ Calibration des seuils et du prompt sur cet échantillon
- ⬜ Rapport d'évaluation de la précision du scoring (livrable §9)

### 4. Tableau de bord (EF-17)
- ✅ Table dénormalisée `dashboard_articles` alimentée par `dag_export_dashboard`
- ⬜ Choix Streamlit vs Metabase
- ⬜ Développement des filtres (source, date, thème, score)
- ⬜ Authentification simple (exigence sécurité §5)

### 5. API d'exposition et exports (EF-18)
- ✅ Export CSV/Excel automatisé dans le pipeline
- ⬜ API FastAPI pour réutilisation interne (objectif §1.2)

### 6. Documentation et livrables finaux (§9)
- ✅ `README.md` (architecture technique)
- ✅ `GETTING_STARTED.md` (guide de démarrage)
- ✅ Ce fichier de suivi
- ⬜ Méthodologie de sélection des sources (document dédié)
- ⬜ Rapport d'évaluation de la précision du scoring
- ⬜ Rapport de fin de mois (bilan, écarts, reports)

### 7. Tests de bout en bout (§10)
- ⬜ Absence de doublons majeurs vérifiée sur données réelles
- ⬜ Cas Figaro (résumé ciblé sur la mention BF, pas l'article entier) testé
- ⬜ Chaque article scoré dispose d'un score exploitable et documenté
- ⬜ Dashboard fonctionnel avec filtres opérationnels

## Prochaine action

1. Lancer `python3 scripts/verify_sources.py --update` sur votre machine pour
   confirmer/corriger les 14 sources encore incertaines.
2. Corriger dans `sources.yaml` les URLs qui échouent (souvent juste le
   chemin du flux qui a changé — le nom de domaine reste bon).
3. Une fois la liste propre, lancer le premier run réel de
   `dag_collecte_quotidienne` (voir `GETTING_STARTED.md` §10).
