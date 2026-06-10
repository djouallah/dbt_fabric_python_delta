{% macro check_new_daily() %}
  {#-- Run-operation the pipeline runner (notebook + CI) calls to decide whether to OVERWRITE
       fct_summary this run. "New daily" = daily files already in the archive log but NOT yet
       ingested into fct_scada — i.e. landing this run, checked BEFORE fct_scada builds.

       Signals through the run-operation's exit status (the runner branches on success):
         - quiet success  -> no new daily -> fct_summary appends intraday
         - raises / fails  -> new daily pending -> runner reruns fct_summary --full-refresh

       Queries the physical OneLake paths directly (not ref()) so it works inside a
       run-operation without the model-relation delta_scan views being registered. The
       duckrun connection already carries the Azure secret minted from ONELAKE_TOKEN. --#}
  {%- if execute -%}
    {%- set log_path  = env_var('FILES_PATH') ~ '/csv_archive_log.parquet' -%}
    {%- set scada_tbl = env_var('ONELAKE_TABLES_PATH') ~ '/landing/fct_scada' -%}
    {%- set q -%}
      SELECT count(*)
      FROM read_parquet('{{ log_path }}')
      WHERE source_type = 'daily'
        AND csv_filename NOT IN (SELECT DISTINCT file FROM delta_scan('{{ scada_tbl }}'))
    {%- endset -%}
    {%- set n = run_query(q).rows[0][0] -%}
    {{ log("pipeline: new daily files pending = " ~ n, info=true) }}
    {%- if n and n > 0 -%}
      {{ exceptions.raise_compiler_error("NEW_DAILY_PENDING") }}
    {%- endif -%}
  {%- endif -%}
{% endmacro %}
