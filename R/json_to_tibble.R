json_to_tibble <- function(
    json_dir = file.path("data", "json")) {
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

  result <- paste0(
    "   SELECT ",
    "       UNNEST(results,  max_depth := 2) ",
    "   FROM ",
    "       read_ndjson('", json_dir, "/*.json', maximum_object_size=1000000000)"
  ) |>
    DBI::dbGetQuery(conn = con) |>
    tibble::as_tibble()

  return(result)
}
