-- Role: ml_group
-- DROP ROLE ml_group;

CREATE ROLE ml_group WITH
  NOLOGIN
  NOSUPERUSER
  INHERIT
  CREATEDB
  NOCREATEROLE
  REPLICATION;


-- Role: ml
-- DROP ROLE ml;

CREATE ROLE ml WITH
  LOGIN
  NOSUPERUSER
  INHERIT
  NOCREATEDB
  NOCREATEROLE
  NOREPLICATION;

GRANT ml_group TO ml;


-- Database: ML_RESULTS

-- DROP DATABASE "ML_RESULTS";

CREATE DATABASE "ML_RESULTS"
    WITH
    OWNER = ml_group
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

GRANT ALL ON DATABASE "ML_RESULTS" TO ml_group;

GRANT TEMPORARY, CONNECT ON DATABASE "ML_RESULTS" TO PUBLIC;


-- Table: results.patient

-- DROP TABLE results.patient;

CREATE TABLE results.patient
(
    patient_id bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    prediction boolean,
    gender character varying(50) COLLATE pg_catalog."default",
    birthday character varying(50) COLLATE pg_catalog."default",
    zip character varying(50) COLLATE pg_catalog."default",
    city character varying(50) COLLATE pg_catalog."default",
    fab character varying(50) COLLATE pg_catalog."default",
    "timestamp" timestamp without time zone,
    CONSTRAINT "PATIENT_pkey" PRIMARY KEY (patient_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE results.patient
    OWNER to ml_group;

GRANT ALL ON TABLE results.patient TO ml_group;


-- Table: results.reason

-- DROP TABLE results.reason;

CREATE TABLE results.reason
(
    patient_id bigint NOT NULL,
    reason character varying(512) COLLATE pg_catalog."default",
    reason_id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    CONSTRAINT "REASONS_pkey" PRIMARY KEY (reason_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE results.reason
    OWNER to ml;

GRANT ALL ON TABLE results.reason TO ml;

GRANT ALL ON TABLE results.reason TO ml_group;