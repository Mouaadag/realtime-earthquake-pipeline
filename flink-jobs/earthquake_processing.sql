-- =============================================================================
--  Flink SQL — Pipeline séismes temps réel (Option A : enrichissement)
--
--  Architecture :
--    Kafka (earthquake-raw)
--      ├── JOB 1 → Kafka (earthquake-enriched)  [enrichissement, SANS fenêtre]
--      └── JOB 2 → Kafka (earthquake-alerts)    [alertes,        SANS fenêtre]
--
--  Pourquoi pas de fenêtre TUMBLE ?
--    Les séismes sont rares (~1 toutes les 15-30 min globalement).
--    Une TUMBLE de 60s produirait des fenêtres vides → rien dans Kafka.
--    Sans fenêtre, chaque événement produit IMMÉDIATEMENT une sortie.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Table SOURCE : lit les données brutes depuis le topic earthquake-raw
--
-- PROCTIME() : horloge système Flink (temps de traitement).
-- scan.startup.mode = 'earliest-offset' : lit depuis le début du topic.
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- Table SINK 1 : séismes enrichis → topic earthquake-enriched
--
-- Champs ajoutés par Flink :
--   depth_category  : classification sismologique (Superficiel / Intermédiaire / Profond)
--   severity        : niveau de danger (Mineur → Dévastateur / TSUNAMI)
--   is_significant  : 1 si significance > 500 (seuil USGS)
--   processed_at    : timestamp de traitement Flink
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- Table SINK 2 : alertes temps réel → topic earthquake-alerts
-- ---------------------------------------------------------------------------


-- ===========================================================================
--  JOB 1 : Enrichissement temps réel — SANS fenêtre
--
--  Chaque événement arrivant dans earthquake-raw produit IMMÉDIATEMENT
--  une ligne enrichie dans earthquake-enriched.
--  Pas de GROUP BY → pas de fenêtre → latence quasi nulle.
-- ===========================================================================



-- ===========================================================================
--  JOB 2 : Alertes temps réel — filtre SANS fenêtre
--
--  Seuls les séismes dangereux (magnitude >= 5.0 ou tsunami) sont émis.
--  Chaque ligne source génère immédiatement une ligne en sortie.
-- ===========================================================================
