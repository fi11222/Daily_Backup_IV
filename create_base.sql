DROP TABLE if exists public."TB_FILE";

CREATE TABLE public."TB_FILE"
(
    "ID_FILE" bigserial,
    "TX_FILE_NAME" text COLLATE pg_catalog."default" NOT NULL,
    "TX_FILE_PATH" text COLLATE pg_catalog."default" NOT NULL,
    "N_LENGTH" bigint NOT NULL,
    "DT_CRE" timestamp(4) without time zone NOT NULL DEFAULT now(),
    "DT_LAST_MOD" timestamp(4) without time zone NOT NULL,
    "S_GROUP" character varying(33) COLLATE pg_catalog."default" NOT NULL,
    "S_OWNER" character varying(33) COLLATE pg_catalog."default" NOT NULL,
    "S_PERMISSIONS" character varying(8) COLLATE pg_catalog."default" NOT NULL,
    "S_EXTENSION" character varying(25) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT "TB_FILE_pkey" PRIMARY KEY ("ID_FILE")
        USING INDEX TABLESPACE ssd_space
)
WITH (
    OIDS = FALSE
)
TABLESPACE ssd_space;

ALTER TABLE public."TB_FILE"
    OWNER to postgres;
	
DROP TABLE if exists public."TB_ACTION";

CREATE TABLE public."TB_ACTION"
(
    "ID_ACTION" bigserial,
    "S_ACTION_TYPE" character varying(1) COLLATE pg_catalog."default" NOT NULL,
    "TX_PATH1" text COLLATE pg_catalog."default" NOT NULL,
    "TX_PATH2" text COLLATE pg_catalog."default",
    "DT_CRE" timestamp(4) without time zone NOT NULL DEFAULT now(),
    CONSTRAINT "TB_ACTION_pkey" PRIMARY KEY ("ID_ACTION")
        USING INDEX TABLESPACE ssd_space
)
WITH (
    OIDS = FALSE
)
TABLESPACE ssd_space;

ALTER TABLE public."TB_ACTION"
    OWNER to postgres;