-- EchoMedia BF — schéma de base (Mois 1)
-- Une table par étape majeure du pipeline (§6.1), pour la traçabilité (EF-16)
-- et pour permettre à chaque DAG de ne lire/écrire que son périmètre.

CREATE TABLE IF NOT EXISTS raw_articles (
    id              BIGSERIAL PRIMARY KEY,
    source_id       TEXT NOT NULL,
    source_type     TEXT NOT NULL,          -- rss | api_gdelt | api_newsapi | scraping
    url             TEXT NOT NULL,
    titre_brut      TEXT,
    texte_brut      TEXT,
    date_publication TIMESTAMPTZ,
    collecte_le     TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id          TEXT NOT NULL,            -- Airflow run_id, pour la traçabilité
    UNIQUE (url)
);

CREATE TABLE IF NOT EXISTS clean_articles (
    id              BIGSERIAL PRIMARY KEY,
    raw_article_id  BIGINT NOT NULL REFERENCES raw_articles(id),
    titre           TEXT,
    texte_propre    TEXT NOT NULL,
    langue          TEXT NOT NULL,           -- fr | en
    hash_contenu    TEXT NOT NULL,           -- pour la déduplication (EF-05)
    est_doublon     BOOLEAN NOT NULL DEFAULT false,
    nettoye_le      TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id          TEXT NOT NULL,
    UNIQUE (hash_contenu)
);

CREATE TABLE IF NOT EXISTS scored_articles (
    id                  BIGSERIAL PRIMARY KEY,
    clean_article_id    BIGINT NOT NULL REFERENCES clean_articles(id),
    passage_pertinent    TEXT,               -- extrait relatif au Burkina Faso (EF-06)
    resume_cible         TEXT,               -- résumé ciblé (EF-07)
    nature_contenu        TEXT,              -- article | depeche | editorial | interview (EF-10)
    themes                TEXT[],            -- liste fermée §2.4 (EF-11)
    orientation           TEXT,              -- favorable | neutre | defavorable
    score_continu         NUMERIC(4,3),      -- -1 à +1
    ton                   TEXT,
    confiance_modele      NUMERIC(4,3),
    entites_personnes     TEXT[],            -- EF-14, Should
    entites_lieux         TEXT[],
    modele_utilise        TEXT,              -- traçabilité du modèle/version (EF-13)
    score_le               TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id                 TEXT NOT NULL
);

-- Table dénormalisée servie au tableau de bord (EF-17), reconstruite par
-- dag_export_dashboard pour ne pas faire porter les jointures au dashboard.
CREATE TABLE IF NOT EXISTS dashboard_articles (
    id                  BIGINT PRIMARY KEY,
    source_id           TEXT NOT NULL,
    source_nom          TEXT,
    titre               TEXT,
    url                 TEXT,
    date_publication    TIMESTAMPTZ,
    langue              TEXT,
    nature_contenu      TEXT,
    themes              TEXT[],
    orientation         TEXT,
    score_continu       NUMERIC(4,3),
    ton                 TEXT,
    resume_cible        TEXT,
    maj_le              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dashboard_source ON dashboard_articles(source_id);
CREATE INDEX IF NOT EXISTS idx_dashboard_date ON dashboard_articles(date_publication);
CREATE INDEX IF NOT EXISTS idx_dashboard_theme ON dashboard_articles USING GIN(themes);
CREATE INDEX IF NOT EXISTS idx_dashboard_orientation ON dashboard_articles(orientation);

-- Journal des exécutions (traçabilité §5, "Fiabilité" et "Traçabilité")
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id          BIGSERIAL PRIMARY KEY,
    dag_id      TEXT NOT NULL,
    task_id     TEXT NOT NULL,
    run_id      TEXT NOT NULL,
    statut      TEXT NOT NULL,             -- success | failed | retried
    details     JSONB,
    horodatage  TIMESTAMPTZ NOT NULL DEFAULT now()
);
