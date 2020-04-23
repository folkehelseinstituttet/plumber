library(pool)
library(data.table)
library(magrittr)
library(ggplot2)

if(.Platform$OS.type == "windows"){
  valid_api_keys <- c("test")
} else {
  apikeys <- readxl::read_excel("/sideloaded_data/apikeys.xlsx")
  setDT(apikeys)
  valid_api_keys <- apikeys[purpose %in% c("covid19_modelling")]$api_key
}

db_config <- list(
  driver = Sys.getenv("DB_DRIVER", "Sql Server"),
  server = Sys.getenv("DB_SERVER", "dm-prod"),
  db = Sys.getenv("DB_DB", "Sykdomspulsen_surv"),
  port = as.integer(Sys.getenv("DB_PORT", 1433)),
  user = Sys.getenv("DB_USER", "root"),
  password = Sys.getenv("DB_PASSWORD", "example")
)

if(db_config$driver %in% c("ODBC Driver 17 for SQL Server")){
  # linux
  pool <- dbPool(
    drv = odbc::odbc(),
    driver = db_config$driver,
    server = db_config$server,
    database = db_config$db,
    port = db_config$port,
    uid = db_config$user,
    Pwd = db_config$password#,
    #trusted_connection = "yes"
  )
} else if(db_config$driver %in% c("Sql Server")){
  # windows
  pool <- dbPool(
    drv = odbc::odbc(),
    driver = db_config$driver,
    server = db_config$server,
    database = db_config$db,
    port = db_config$port,
    trusted_connection = "yes"
  )
} else {
  pool <- dbPool(
    drv = odbc::odbc(),
    driver = db_config$driver,
    server = db_config$server,
    port = db_config$port,
    user = db_config$user,
    password = db_config$password,
    encoding = "utf8"
  )
}
DBI::dbExecute(pool, glue::glue({"USE {db_config$db};"}))




#* Filter that grabs the "username" querystring parameter.
#* You should, of course, use a real auth system, but
#* this shows the principles involved.
#* @filter auth-user
function(req, username=""){
  # Since username is a querystring param, we can just
  # expect it to be available as a parameter to the
  # filter (plumber magic).

  req$valid_key <- FALSE

  api_key <- req$args$api_key

  if (is.null(api_key)){
    # No username provided
    #stop("No api_key provided")
  } else if (api_key %in% valid_api_keys){
    # username is valid
    req$valid_key <- TRUE
  } else {
    # username was provided, but invalid
    #stop("No such username: ", username)
  }

  # Continue on
  forward()
}

#* Now require that all users must be authenticated.
#* @filter require-auth
function(req, res){
  if (is.null(req$valid_key)){
    # User isn't logged in
    res$status <- 401 # Unauthorized
    list(error="You must login to access this resource.")
  } else if(req$valid_key==FALSE){
    # User isn't logged in
    res$status <- 401 # Unauthorized
    list(error="You must login to access this resource.")
  } else {
    # user is logged in. Move on...
    forward()
  }
}

#* These are the locations and location names
#* @param api_key api_key
#* @get /model_msis_cases_by_time_location
function(req, res, api_key){
  d <- pool %>% dplyr::tbl("data_covid19_msis_by_time_location") %>%
    dplyr::filter(granularity_time == "day") %>%
    dplyr::filter(granularity_geo == "county") %>%
    dplyr::select(location_code, date, n) %>%
    dplyr::collect()
  setDT(d)
  d
}