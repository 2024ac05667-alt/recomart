CREATE SCHEMA IF NOT EXISTS recomart
    AUTHORIZATION postgres;


-- Table: recomart.feature_metadata

-- DROP TABLE IF EXISTS recomart.feature_metadata;

CREATE TABLE IF NOT EXISTS recomart.feature_metadata
(
    feature_name character varying(50) COLLATE pg_catalog."default" NOT NULL,
    source_table character varying(50) COLLATE pg_catalog."default",
    transformation character varying(200) COLLATE pg_catalog."default",
    last_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT feature_metadata_pkey PRIMARY KEY (feature_name)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS recomart.feature_metadata
    OWNER to postgres;



-- Table: recomart.feature_store

-- DROP TABLE IF EXISTS recomart.feature_store;

CREATE TABLE IF NOT EXISTS recomart.feature_store
(
    user_id bigint,
    item_id bigint,
    avg_user_rating double precision,
    avg_item_rating double precision,
    user_activity_count bigint,
    co_occurrence_count bigint,
    last_updated timestamp without time zone
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS recomart.feature_store
    OWNER to postgres;




-- Table: recomart.model_metadata

-- DROP TABLE IF EXISTS recomart.model_metadata;

CREATE TABLE IF NOT EXISTS recomart.model_metadata
(
    run_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    training_date timestamp without time zone,
    rmse double precision,
    n_features integer,
    model_type character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT model_metadata_pkey PRIMARY KEY (run_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS recomart.model_metadata
    OWNER to postgres;


-- Table: recomart.raw_interactions

-- DROP TABLE IF EXISTS recomart.raw_interactions;

CREATE TABLE IF NOT EXISTS recomart.raw_interactions
(
    user_id bigint,
    item_id bigint,
    rating bigint,
    ingested_at timestamp with time zone
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS recomart.raw_interactions
    OWNER to postgres;


-- Table: recomart.raw_products

-- DROP TABLE IF EXISTS recomart.raw_products;

CREATE TABLE IF NOT EXISTS recomart.raw_products
(
    item_id bigint,
    category text COLLATE pg_catalog."default",
    price double precision
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS recomart.raw_products
    OWNER to postgres;