#!/usr/bin/env bash
# À exécuter une fois l'environnement Airflow démarré (docker compose up -d),
# depuis l'intérieur du conteneur webserver ou scheduler :
#   docker compose exec airflow-webserver bash scripts/init_airflow_setup.sh
set -euo pipefail

echo "== Connexion à la base métier =="
airflow connections add 'echomedia_db' \
    --conn-type 'postgres' \
    --conn-host 'postgres_echomedia' \
    --conn-schema 'echomedia' \
    --conn-login 'echomedia' \
    --conn-password 'echomedia' \
    --conn-port '5432' \
  || echo "connexion echomedia_db déjà existante, ignorée"

echo "== Variables Airflow (paramétrage exposé dans l'UI, cf. EF-02) =="
airflow variables set ECHOMEDIA_CONFIG_DIR "/opt/airflow/config"
airflow variables set ECHOMEDIA_LANGUES_SUIVIES "fr,en"
airflow variables set ECHOMEDIA_VOLUME_CIBLE_JOUR "300"

echo "== Rappel : secrets à définir en variables d'environnement (jamais en dur) =="
echo "  NEWSAPI_KEY, GNEWS_KEY, LLM_API_KEY -> dans le fichier .env (voir .env.example)"

echo "Setup terminé."
