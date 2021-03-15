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
    server = "dm-test",
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

mandatory_db_filter <- function(
  .data,
  granularity_time = NULL,
  granularity_time_not = NULL,
  granularity_geo = NULL,
  granularity_geo_not = NULL,
  age = NULL,
  age_not = NULL,
  sex = NULL,
  sex_not = NULL
){
  retval <- .data

  if(!is.null(granularity_time)) retval <- retval %>% dplyr::filter(granularity_time %in% !!granularity_time)
  if(!is.null(granularity_time_not)) retval <- retval %>% dplyr::filter(!granularity_time %in% !!granularity_time_not)

  if(!is.null(granularity_geo)) retval <- retval %>% dplyr::filter(granularity_geo %in% !!granularity_geo)
  if(!is.null(granularity_geo_not)) retval <- retval %>% dplyr::filter(!granularity_geo %in% !!granularity_geo_not)

  if(!is.null(age)) retval <- retval %>% dplyr::filter(age %in% !!age)
  if(!is.null(age_not)) retval <- retval %>% dplyr::filter(!age %in% !!age_not)

  if(!is.null(sex)) retval <- retval %>% dplyr::filter(sex %in% !!sex)
  if(!is.null(sex_not)) retval <- retval %>% dplyr::filter(!sex %in% !!sex_not)

  return(retval)
}



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
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /model_msis_cases_by_time_location
function(req, res, api_key, prelim=F){
  d <- pool %>% dplyr::tbl("data_autoc19_msis_by_time_location") %>%
    mandatory_db_filter(
      granularity_time = "day",
      granularity_geo = "county",
      age = "total",
      sex = "total"
    ) %>%
    dplyr::select(location_code, date, n=n_pr) %>%
    dplyr::collect()
  setDT(d)
  d
}

#* These are the locations and location names
#* @param api_key api_key
#* @get /model_norsyss_covid19_by_time_location
function(req, res, api_key){
  d <- pool %>% dplyr::tbl("data_norsyss") %>%
    mandatory_db_filter(
      granularity_time = "day",
      granularity_geo = c("nation","county"),
      age = "total",
      sex = "total"
    ) %>%
    dplyr::filter(tag_outcome %in% "covid19_vk_ote") %>%
    dplyr::filter(date >= "2020-03-09") %>%
    dplyr::select(location_code, date, n, consult_with_influenza) %>%
    dplyr::collect()
  setDT(d)
  setorder(d,location_code,date)
  d
}

#* (G) model_hospital_by_time_location ----
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /model_hospital_by_time_location
function(req, res, api_key, prelim=F){

  d <- pool %>% dplyr::tbl("data_autoc19_hospital_by_time_location") %>%
    mandatory_db_filter(
      granularity_time = "day",
      granularity_geo = c("nation", "county"),
      age = "total",
      sex = "total"
    ) %>%
    dplyr::select(location_code, date, n_hospital_main_cause) %>%
    dplyr::collect()
  setDT(d)
  d[,date:=as.Date(date)]
  setorder(d, location_code, date)

  d
}

#* (I) hc_icu_by_time_location ----
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /model_icu_by_time_location
function(req, res, api_key, prelim=FALSE){

  d <- pool %>% dplyr::tbl("data_autoc19_hospital_by_time_location") %>%
    mandatory_db_filter(
      granularity_time = "day",
      granularity_geo = "nation",
      age = "total",
      sex = "total"
    ) %>%
    dplyr::select(date, n_icu) %>%
    dplyr::collect()
  setDT(d)
  d[,date:=as.Date(date)]
  setorder(d, date)

  d
}
