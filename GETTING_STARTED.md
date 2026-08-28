# Guide de démarrage — EchoMedia BF sur Linux, en partant de zéro

Ce guide contient **toutes les commandes**, dans l'ordre, du tout premier
`unzip` jusqu'à un pipeline qui tourne avec des données réelles. Copiez-collez
dans l'ordre. Chaque section indique si elle nécessite Docker ou juste Python.

## 1. Le concept, en une image

Le projet fait tourner 3 "boîtes" (conteneurs Docker) en parallèle :
- **Airflow** (le chef d'orchestre) — décide quand et dans quel ordre les
  tâches s'exécutent.
- **Une base Postgres pour Airflow** — sa propre mémoire interne (logs,
  historique des runs).
- **Une base Postgres "métier"** (`postgres_echomedia`) — où vos articles
  collectés, nettoyés et scorés sont stockés.

Le pipeline est composé de 4 DAGs qui se passent le relais **automatiquement**,
chacun écrivant dans une table que le suivant lit :

```
dag_collecte_quotidienne   →  raw_articles
        ↓ (déclenche automatiquement le suivant)
dag_nettoyage_dedup        →  clean_articles
        ↓
dag_classification_scoring →  scored_articles
        ↓
dag_export_dashboard       →  dashboard_articles + fichiers CSV/Excel
```

Vous n'avez rien à déclencher manuellement après le premier run : dès qu'un
DAG termine et "publie" son résultat (mécanisme des *Airflow Datasets*),
Airflow lance le suivant tout seul.

---

## 2. Prérequis — vérifier ce qui est déjà installé

```bash
docker --version
docker compose version
python3 --version
```

- Si `docker` n'existe pas : installez-le via
  [docs.docker.com/engine/install](https://docs.docker.com/engine/install/)
  (choisissez votre distribution — Ubuntu, Debian, Fedora...).
- Après installation, ajoutez votre utilisateur au groupe `docker` pour
  éviter `sudo` à chaque commande :
  ```bash
  sudo usermod -aG docker $USER
  newgrp docker   # ou déconnectez-vous/reconnectez-vous
  ```
- `docker compose` (sans tiret) est inclus dans les versions récentes de
  Docker. Si `docker compose version` échoue, testez `docker-compose
  --version` (ancienne syntaxe) — dans ce cas remplacez `docker compose` par
  `docker-compose` dans toutes les commandes de ce guide.
- `python3` est presque toujours déjà présent sur Linux. Sinon :
  `sudo apt install python3 python3-pip` (Debian/Ubuntu).

---

## 3. Récupérer et inspecter le projet

```bash
mkdir -p ~/projets && cd ~/projets
unzip echomedia_airflow.zip
cd echomedia_airflow
ls -la
```

Vous devez voir `docker-compose.yml`, `config/`, `dags/`, `sql/`, `scripts/`,
`README.md`, `SUIVI_PROJET.md`.

---

## 4. Configurer les secrets (clés API)

**Ne sautez pas cette étape**, sinon `dag_collecte_quotidienne` et
`dag_classification_scoring` planteront à l'exécution.

```bash
cp .env.example .env
nano .env    # ou vim, ou l'éditeur que vous préférez
```

Remplissez :
```
NEWSAPI_KEY=votre_clé_newsapi
LLM_API_KEY=votre_clé_api_anthropic
```

- `NEWSAPI_KEY` : créez un compte gratuit sur [newsapi.org](https://newsapi.org)
- `LLM_API_KEY` : votre clé API Anthropic (console.anthropic.com), utilisée
  par `dag_classification_scoring` pour le résumé/scoring
- `GNEWS_KEY` peut rester vide si vous n'utilisez pas GNews en M1

---

## 5. Vérifier les sources médiatiques (sans Docker)

Avant de lancer quoi que ce soit, on vérifie que les flux RSS et pages de
`config/sources.yaml` répondent réellement. Ça se fait en Python pur, pas
besoin de Docker pour cette étape :

```bash
pip install httpx feedparser pyyaml --break-system-packages
python3 scripts/verify_sources.py --update
```

Le script affiche ✅/❌ pour chaque source et corrige automatiquement le champ
`verifie:` dans `sources.yaml` (option `--update`). Corrigez dans le fichier
les URLs marquées ❌ (souvent juste le chemin du flux qui a changé), puis
relancez la commande jusqu'à ce que le maximum de sources passe au vert.
Les deux lignes ⚠️ (`gdelt`, `newsapi`) sont normales : ce sont des API qui
nécessitent une clé, testées plus loin à l'étape 11.

---

## 6. Premier démarrage Docker — initialisation

Cette étape crée la base de données Airflow, l'utilisateur admin, et installe
les dépendances Python du pipeline (`requirements.txt`). **Elle ne se fait
qu'une seule fois** :

```bash
docker compose up airflow-init
```

Ça va prendre quelques minutes (installation de pandas, spacy, trafilatura,
etc.). Vous devez voir à la fin un message de succès sans erreur rouge. Si ça
échoue sur `pip install`, c'est probablement un problème réseau ou une
dépendance manquante.

---

## 7. Lancer les services

```bash
docker compose up -d
```

Le `-d` = en arrière-plan (detached). Vérifiez que tout tourne :

```bash
docker compose ps
```

Vous devez voir 4 conteneurs `Up` : `postgres_airflow`, `postgres_echomedia`,
`airflow-webserver`, `airflow-scheduler`.

---

## 8. Créer la connexion et les variables Airflow

```bash
docker compose exec airflow-webserver bash scripts/init_airflow_setup.sh
```

Ce script dit à Airflow "voici comment te connecter à la base métier
`postgres_echomedia`" et enregistre quelques paramètres. Sans ça, les DAGs ne
trouveront pas la base de données.

---

## 9. Ouvrir l'interface Airflow

Dans votre navigateur : **http://localhost:8080**
Identifiants : `admin` / `admin` (définis dans `docker-compose.yml`, à
changer si vous exposez ça publiquement un jour).

Vous verrez la liste des 4 DAGs, tous **désactivés par défaut** (interrupteur
gris à gauche du nom). C'est normal, Airflow ne lance jamais un DAG tout seul
sans que vous l'activiez.

---

## 10. Activer les DAGs

Basculez les 4 interrupteurs sur "On" dans l'interface. Seul
`dag_collecte_quotidienne` a un vrai horaire (tous les jours à 5h). Les 3
autres attendent passivement que le précédent ait fini — vous pouvez les
activer tous en même temps, rien ne se passera tant que la collecte n'a pas
tourné.

---

## 11. Tester tout de suite sans attendre 5h du matin

Dans l'interface Airflow, cliquez sur `dag_collecte_quotidienne` → bouton ▶️
"Trigger DAG" en haut à droite. Ça va :
1. Aller chercher les flux RSS/API définis dans `config/sources.yaml`
2. Filtrer ce qui parle du Burkina Faso
3. Écrire dans `raw_articles`
4. Déclencher automatiquement `dag_nettoyage_dedup`, puis
   `dag_classification_scoring`, puis `dag_export_dashboard` en cascade

Vous pouvez suivre la progression dans l'onglet "Graph" de chaque DAG (cases
vertes = succès, rouges = échec). C'est ici que les DAGs `gdelt` et `newsapi`
sont réellement testés avec vos clés API.

---

## 12. Vérifier les données produites

```bash
docker compose exec postgres_echomedia psql -U echomedia -d echomedia -c "SELECT count(*) FROM raw_articles;"
docker compose exec postgres_echomedia psql -U echomedia -d echomedia -c "SELECT titre, orientation, score_continu FROM dashboard_articles LIMIT 10;"
```

Les exports CSV/Excel sont dans le conteneur à `/opt/airflow/exports` — pour
les récupérer sur votre machine :

```bash
docker compose cp airflow-scheduler:/opt/airflow/exports ./exports_locaux
```

---

## 13. Commandes utiles pour la suite

```bash
docker compose logs -f airflow-scheduler   # suivre les logs en direct
docker compose down                        # arrêter tous les conteneurs
docker compose down -v                     # arrêter ET supprimer les données (repart de zéro)
docker compose restart airflow-scheduler   # relancer juste le scheduler après une modif de DAG
python3 scripts/verify_sources.py --update # re-tester les sources après une modif de sources.yaml
```

---

## 14. Ce qui est prêt vs ce qui reste à ajuster

- ✅ Structure, orchestration, chaînage, base de données : fonctionnels tels
  quels.
- ⚠️ `fasttext` a besoin d'un modèle téléchargé (`lid.176.ftz`) qui n'est pas
  inclus dans le projet — sans lui, le DAG utilise un fallback grossier
  (détection FR/EN approximative). À télécharger depuis le site fastText si
  vous voulez la vraie précision.
- ⚠️ Le prompt du LLM dans `dag_classification_scoring.py` est un point de
  départ — à affiner avec de vrais articles collectés.

Voir `SUIVI_PROJET.md` pour le détail fait/à faire sur l'ensemble du projet,
pas seulement le pipeline technique.
