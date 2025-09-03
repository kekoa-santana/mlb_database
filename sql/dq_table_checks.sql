-- dq_table_checks.sql
-- Params you pass at runtime:
--  -v schema=public -v table=mlb_boxscores -v pk=game_pk -v datecol=game_date

\timing on 

-- ------- Validate inputs & set defaults ------

\if :{?schema}
\else
    \set schema 'public'
\endif

\if :{?table}
\else
    \echo 'ERROR: you must pass -v table=<table_name>' \quit 3
\endif

\if :{?pk}
\else
    \echo 'ERROR: you must pass -v pk=<primary_key_column>' \quit 3
\endif

\if :{?datecol}
    \set _want_date 1
\else
    \set _want_date 0
\endif

-- Verify table exists
SELECT COUNT(*) AS _table_exists
FROM information_schema.tables 
WHERE table_schema = :'schema' AND table_name = :'table' \gset

\if :_table_exists
\else
    \echo 'ERROR: table ' :'schema' '.' :'table' ' not found' \quit 3
\endif

-- Check if the date column exists (optional)
SELECT CASE WHEN COUNT(*)>0 THEN 1 ELSE 0 END AS _has_date
FROM information_schema.columns
WHERE table_schema=:'schema' AND table_name=:'table' AND column_name=:'datecol' \gset

\echo === Running DQ on :"schema".:"table" (pk=:"pk") ===


-- ------ A) NULL checks ------
\echo == Null  key/date check ==
WITH t AS (
    SELECT * FROM :"schema".:"table"
)
SELECT :'table'::text AS "table",
    COUNT(*) FILTER (WHERE t.:"pk" IS NULL) AS null_pk,
    CASE WHEN :_want_date::int=1 AND :_has_date::int=1
        THEN COUNT(*) FILTER (WHERE t.:"datecol" IS NULL)
        ELSE NULL
    END AS null_date
FROM t;

-- ------ B) Duplicate key check ------
\echo == Duplicate :"pk" check ==
SELECT t.:"pk" AS key_value, COUNT(*) AS rows_per_key
FROM :"schema".:"table" AS t
GROUP BY t.:"pk"
HAVING COUNT(*) > 1
ORDER BY rows_per_key DESC
LIMIT 50;