CREATE OR REPLACE PROCEDURE  dwh.dq_rule_log_insert(IN RULE_QUERY VARCHAR,IN RULE_COUNT INT)
LANGUAGE plpgsql
AS $$
DECLARE 
	cnt INTEGER := 0;
BEGIN
  <<simple_loop_exit_continue>>
  LOOP
	EXECUTE 'RULE_QUERY'  ;  
	cnt = cnt + 1;
    EXIT simple_loop_exit_continue WHEN (cnt > RULE_COUNT);
  END LOOP;
  RAISE INFO 'Loop Statement Executed!!!';
END;
$$;

CREATE TABLE dwh.data_quality_error_log(
rule_id int, 
INTCOLUMN1 int,
INTCOLUMN2 int,
INTCOLUMN3 int,
STRCOLUMN1 varchar(4000),
STRCOLUMN2  varchar(4000),
STRCOLUMN3 varchar(4000),
log_date datetime  
);

CREATE truncate TABLE DWH.DATA_QUALITY_RULES(
rule_id int,
rule_desc varchar(4000),
rule_query  varchar(4000)
)
