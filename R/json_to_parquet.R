json_to_parquet <- function(
    json_dir = file.path("data", "json"),
    arrow_dir = file.path("data", "data")) {
  ## Define set of json files

  ## Create in memory DuckDB
  con <- DBI::dbConnect(duckdb::duckdb())

  on.exit(
    DBI::dbDisconnect(con, shutdown = TRUE)
  )

  ## Install and load jsonq
  paste0(
    "INSTALL json"
  ) |>
    DBI::dbExecute(conn = con)

  paste0(
    "LOAD json"
  ) |>
    DBI::dbExecute(conn = con)

  paste0(
    "COPY ( ",
    "   SELECT ",
    "       UNNEST(results,  max_depth := 2) ",
    "   FROM ",
    "       read_ndjson('", json_dir, "/*.json')",
    ") TO '", arrow_dir, "' ",
    "(FORMAT PARQUET, COMPRESSION SNAPPY, PARTITION_BY 'publication_year')"
  ) |>
    DBI::dbExecute(conn = con)
}
