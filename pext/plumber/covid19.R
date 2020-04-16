library(pool)
library(data.table)
library(magrittr)
library(ggplot2)

if(.Platform$OS.type == "windows"){
  valid_api_keys <- c("test")
} else {
  apikeys <- readxl::read_excel("/sideloaded_data/apikeys.xlsx")
  setDT(apikeys)
  valid_api_keys <- apikeys[purpose %in% c("covid19")]$api_key
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
#* @param location_code location code ("norge" is a common choice)
#* @param api_key api_key
#* @get /hc_reported_cases
#* @serializer highcharts
function(req, res, api_key, location_code){
  if(location_code!="norge") stop("not valid")
  d <- pool %>% dplyr::tbl("data_covid19_msis") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(date, n) %>%
    dplyr::collect()
  setDT(d)
  #d[,date:=as.POSIXlt(date)+0.001]
  d[,date:=as.Date(date)]
  setorder(d,date)
  d[,cum_n := cumsum(n)]

  setcolorder(d,c("date","cum_n","n"))
  setnames(d, c("Dato", "Antall", "Nye i dag"))

  d
  # j1 <- jsonlite::toJSON(names(d), dataframe = c("values"))
  # j2 <- jsonlite::toJSON(d, dataframe = c("values"))
  # j2 <- stringr::str_remove(j2,"\\[")
  #
  # j <- paste0("[",j1,",",j2)
  #
  # res$body <- "hello"
  # res
}

#* These are the locations and location names
#* @param location_code location code ("norge" is a common choice)
#* @param granularity_time day or week
#* @param api_key api_key
#* @get /hc_msis_cases_by_time_location
#* @serializer highcharts
function(req, res, api_key, granularity_time, location_code){
  stopifnot(granularity_time %in% c("day","week"))
  d <- pool %>% dplyr::tbl("data_covid19_msis") %>%
    dplyr::filter(granularity_time == !!granularity_time) %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(yrwk, date, n) %>%
    dplyr::collect()
  setDT(d)
  d[,date:=as.Date(date)]
  setorder(d, date)

  d[,cum_n := cumsum(n)]

  setcolorder(d,c("yrwk", "date","cum_n","n"))
  if(granularity_time=="day"){
    d[, yrwk:=NULL]
  } else {
    d[, date:=NULL]
  }
  setnames(d, c(glue::glue("Pr{fhi::nb$oe}vedato"), "Antall", "Nye"))
  d
}
#* These are the locations and location names
#* @param location_code location_code
#* @param api_key api_key
#* @get /hc_msis_cases_by_time_infected_location
#* @serializer highcharts
function(req, res, api_key, location_code){
  d <- data.table(
    Ukenr = c(
      "2020-02",
      "2020-03",
      "2020-04",
      "2020-05"
    )
  )
  d[,Norge := rpois(.N, lambda = 100)]
  d[,Utlandet := rpois(.N, lambda = 100)]
  d[,Ukjent := rpois(.N, lambda = 100)]
  d
  # j1 <- jsonlite::toJSON(names(d), dataframe = c("values"))
  # j2 <- jsonlite::toJSON(d, dataframe = c("values"))
  # j2 <- stringr::str_remove(j2,"\\[")
  #
  # j <- paste0("[",j1,",",j2)
  #
  # res$body <- "hello"
  # res
}

#* These are the locations and location names
#* @param location_code location_code
#* @param api_key api_key
#* @get /hc_msis_cases_by_age
#* @serializer highcharts
function(req, res, api_key, location_code){
  d <- data.table(
    Aldersgruppe = c(
      "0-9",
      "10-19",
      "20-29",
      "30-39",
      "40-49",
      "50-59",
      "60-69",
      "70-79",
      "80-89",
      "90+"
    )
  )
  d[,Insidensrate := rpois(.N, lambda = 4)]

  d
}

#* These are the locations and location names
#* @param location_code location_code
#* @param api_key api_key
#* @get /hc_msis_cases_by_sex
#* @serializer highcharts
function(req, res, api_key, location_code){
  d <- data.table(
    sex = c("Menn", "Kvinner"),
    antall = c(40,30)
  )

  setnames(
    d,
    c(
      glue::glue("Kj{fhi::nb$oe}nn"),
      "Antall"
    )
  )

  d
  # j1 <- jsonlite::toJSON(names(d), dataframe = c("values"))
  # j2 <- jsonlite::toJSON(d, dataframe = c("values"))
  # j2 <- stringr::str_remove(j2,"\\[")
  #
  # j <- paste0("[",j1,",",j2)
  #
  # res$body <- "hello"
  # res
}

#* These are the locations and location names
#* @param api_key api_key
#* @preempt require-auth
#* @get /locations
function(req, api_key){
  # Safe to assume we have a user, since we've been
  # through all the filters and would have gotten an
  # error earlier if we weren't.
  list(data=fhidata::norway_locations_long_b2020)
}

