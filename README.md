# EchoMedia BF — Pipeline Airflow (Mois 1)

Implémentation des 4 DAGs prévus au §6.4 du cahier des charges, avec chaînage
automatique, dates de démarrage, paramétrage externalisé et journalisation.

## 1. Démarrage

```bash
cp .env.example .env        # puis renseigner NEWSAPI_KEY / GNEWS_KEY / LLM_API_KEY
docker compose up airflow-init      # une seule fois : migration DB + user admin + connexion
docker compose up -d                # webserver (localhost:8080, admin/admin) + scheduler
docker compose exec airflow-webserver bash scripts/init_airflow_setup.sh
```

Le schéma SQL (`sql/schema.sql`) est appliqué automatiquement au premier
démarrage du conteneur `postgres_echomedia`.

## 2. Chaînage des 4 DAGs — dates et déclenchement

| DAG | Déclenchement | start_date | catchup |
|---|---|---|---|
| `dag_collecte_quotidienne` | Cron `0 5 * * *` (quotidien, 05h00) | 2026-08-25 | False |
| `dag_nettoyage_dedup` | Dataset `raw_articles` (publié par le DAG précédent) | 2026-08-25 | False |
| `dag_classification_scoring` | Dataset `clean_articles` | 2026-08-25 | False |
| `dag_export_dashboard` | Dataset `scored_articles` | 2026-08-25 | False |

**Pourquoi des Datasets et pas des `TriggerDagRunOperator`** : chaque DAG
« publie » un Dataset en fin de run (`outlets=[...]`) et le DAG suivant est
programmé avec `schedule=[Dataset]`. Airflow le déclenche automatiquement dès
que le Dataset est mis à jour — pas de couplage dur entre DAGs, et un DAG en
échec ne déclenche pas la suite (contrairement à un trigger explicite mal
protégé). Seul `dag_collecte_quotidienne` a un vrai cron ; les 3 suivants sont
100% pilotés par les données produites.

`start_date=2026-08-25` correspond à la mise en route du pipeline pilote
(§2.2 : "la collecte démarre à la mise en route du pipeline, sans
reconstitution rétroactive"). **À ajuster à la date réelle de mise en
production.** `catchup=False` partout : on ne rejoue jamais les jours passés
avant la mise en route.

## 3. Paramétrage sans toucher au code

- `config/sources.yaml` : liste des sources, type (rss/api/scraping), langue,
  actif/inactif — modifiable par un analyste (EF-02).
- `config/scoring.yaml` : thèmes (§2.4), tons, seuils de l'échelle de scoring
  (§11.1), mots-clés de pertinence (EF-03).
- Airflow Variables (`ECHOMEDIA_LANGUES_SUIVIES`, `ECHOMEDIA_VOLUME_CIBLE_JOUR`)
  pour les paramètres consultables/modifiables depuis l'UI Airflow.
- Secrets (clés API) : jamais dans ces fichiers YAML — dans `.env`, injectés
  comme variables d'environnement dans les conteneurs.

## 4. Ce que fait chaque DAG

**dag_collecte_quotidienne** — 3 task groups en parallèle (`collecte_rss` en
mapping dynamique sur les sources actives, `collecte_api` pour
GDELT/NewsAPI, `collecte_scraping`), puis une task de clôture qui publie le
Dataset. Filtrage de pertinence (mention explicite du Burkina Faso) appliqué
dès la collecte pour ne pas stocker de bruit.

**dag_nettoyage_dedup** — récupère les articles bruts pas encore traités,
extrait le texte propre, détecte la langue (fastText, fallback heuristique
si le modèle n'est pas chargé), déduplique par hash de contenu **et**
similarité de titre, écrit dans `clean_articles`.

**dag_classification_scoring** — pour chaque article non scoré : un appel
LLM (mapping dynamique `.expand`, un article = une task, échecs isolés) qui
renvoie en une passe le passage pertinent, le résumé ciblé, la nature, les
thèmes, l'orientation, le score continu, le ton, la confiance et les entités.
L'orientation est recalée côté code à partir des seuils de `scoring.yaml`
pour rester cohérente même si le modèle hésite.

**dag_export_dashboard** — reconstruit `dashboard_articles` (table
dénormalisée, upsert) puis génère les exports CSV/Excel horodatés dans
`/opt/airflow/exports`.

## 5. Points ouverts à trancher en semaine 1/3 (cf. cahier des charges)

- Liste définitive des sources (§2.3) → `config/sources.yaml` à compléter.
- Seuils de l'échelle de scoring (§11.1) → à valider sur corpus annoté en S3.
- Choix définitif du modèle LLM de scoring et de son prompt exact → à
  calibrer sur l'échantillon annoté (rapport de précision, §9).
- Notification en cas d'échec (`task_failure_alert` journalise seulement pour
  l'instant — brancher Slack/e-mail si souhaité, hors périmètre M1 EF-19).

## 6. Structure du repo

```
echomedia_airflow/
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── config/
│   ├── sources.yaml
│   └── scoring.yaml
├── sql/
│   └── schema.sql
├── scripts/
│   └── init_airflow_setup.sh
└── dags/
    ├── common/
    │   ├── config.py
    │   └── db.py
    ├── dag_collecte_quotidienne.py
    ├── dag_nettoyage_dedup.py
    ├── dag_classification_scoring.py
    └── dag_export_dashboard.py
```
