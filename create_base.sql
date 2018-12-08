DROP TABLE if exists public."TB_FILE";

CREATE TABLE public."TB_FILE"
(
  "TX_FILE_NAME" text NOT NULL,
  "TX_FILE_PATH" text NOT NULL,
  "N_LENGTH" bigint,
  "DT_LAST_MOD" timestamp without time zone,
  "S_GROUP" character varying(20),
  "S_OWNER" character varying(20),
  "S_PERMISSIONS" character varying(3),
  "S_EXTENSION" character varying(10),
  CONSTRAINT "TB_FILE_pkey" PRIMARY KEY ("TX_FILE_NAME", "TX_FILE_PATH")
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public."TB_FILE"
  OWNER TO postgres;

DROP TABLE if exists public."TB_ACTION";

CREATE TABLE public."TB_ACTION"
(
  "ID_ACTION" bigserial,
  "S_ACTION_TYPE" character varying(1) NOT NULL,
  "TX_PATH1" text,
  "TX_PATH2" text,
  "ID_CYCLE" uuid NOT NULL,
  CONSTRAINT "TB_ACTION_pkey" PRIMARY KEY ("ID_ACTION")
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public."TB_ACTION"
  OWNER TO postgres;

DROP TABLE if exists public."TB_CYCLE";

CREATE TABLE public."TB_CYCLE"
(
  "ID_CYCLE" uuid NOT NULL,
  "DT_START" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  "DT_END" timestamp without time zone,
  CONSTRAINT "TB_CYCLE_pkey" PRIMARY KEY ("ID_CYCLE")
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public."TB_CYCLE"
  OWNER TO postgres;