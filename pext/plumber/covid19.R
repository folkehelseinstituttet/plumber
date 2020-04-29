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

#* These key numbers
#* @param location_code location code ("norge" is a common choice)
#* @param lang nb or en
#* @param api_key api_key
#* @get /hc_key_numbers
function(req, res, api_key, lang="nb", location_code){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  n_msis <- pool %>% dplyr::tbl("data_covid19_msis_by_time_location") %>%
    dplyr::filter(granularity_time == "day") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::summarize(n=sum(n)) %>%
    dplyr::collect()
  n_msis <- n_msis$n

  n_icu <- pool %>% dplyr::tbl("data_covid19_nir_by_time") %>%
    dplyr::filter(granularity_time == "day") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::filter(date==max(date,na.rm=T)) %>%
    dplyr::select(date, n_icu) %>%
    dplyr::collect()
  n_icu <- n_icu$n_icu

  n_lab <- pool %>% dplyr::tbl("data_covid19_lab_by_time") %>%
    dplyr::filter(granularity_time == "day") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::summarize(n = sum(n_neg + n_pos)) %>%
    dplyr::collect()
  n_lab <- n_lab$n
  n_lab

  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task=="data_covid19_daily_report") %>%
    dplyr::select("date") %>%
    dplyr::collect()
  last_mod <- last_mod$date
  last_mod <- format.Date(last_mod, "%d/%m/%Y")

  list(
    figures = rbind(
      data.frame(
        key = "n_msis",
        number = n_msis,
        description = "SMITTEDE",
        updated = last_mod
      ),

      data.frame(
        key = "n_icu",
        number = n_icu,
        description = "ICU",
        updated = last_mod
      ),

      data.frame(
        key = "n_lab",
        number = n_lab,
        description = "TESTET",
        updated = last_mod
      )
    )
  )
}


#* These are the locations and location names
#* @param location_code location code ("norge" is a common choice)
#* @param granularity_time day or week
#* @param lang nb or en
#* @param api_key api_key
#* @get /hc_msis_cases_by_time_location
#* @serializer highcharts
function(req, res, api_key, lang="nb", granularity_time, location_code){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(granularity_time %in% c("day","week"))

  valid_locations <- unique(fhidata::norway_locations_b2020$county_code)
  valid_locations <- stringr::str_remove(valid_locations, "county")
  valid_locations <- c("norge", valid_locations)
  stopifnot(location_code %in% valid_locations)

  if(stringr::str_length(location_code)==2) location_code <- paste0("county",location_code)

  d <- pool %>% dplyr::tbl("data_covid19_msis_by_time_location") %>%
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

    if(lang=="nb"){
      setnames(d, c(glue::glue("Pr{fhi::nb$oe}vetakingsdato"), "Kumulativt antall", "Nye tilfeller"))
    } else {
      setnames(d, c(glue::glue("Sampling date"), "Cumulative cases", "New cases"))
    }
  } else {
    d[, date:=NULL]

    if(lang=="nb"){
      setnames(d, c(glue::glue("Pr{fhi::nb$oe}vetakingsuke"), "Kumulativt antall", "Nye tilfeller"))
    } else {
      setnames(d, c(glue::glue("Sampling week"), "Cumulative cases", "New cases"))
    }
  }

  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task=="data_covid19_daily_report") %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}

#* These are the locations and location names
#* @param location_code location_code ("norge")
#* @param lang nb or en
#* @param api_key api_key
#* @get /hc_msis_cases_by_age_sex
#* @serializer highcharts
function(req, res, api_key, lang="nb", location_code){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl("data_covid19_msis_by_sex_age") %>%
    dplyr::filter(granularity_time == "total") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(age, sex, n) %>%
    dplyr::collect()
  setDT(d)

  d <- dcast.data.table(d, age ~ sex, value.var = "n")
  d[, total := female + male]
  setnames(
    d,
    c(
      "Alder",
      "Kvinner",
      "Menn",
      "Totalt"
    )
  )
  setcolorder(
    d,
    c(
      "Alder",
      "Totalt"
    )
  )

  if(lang=="en"){
    setnames(
      d,
      c(
        "Age",
        "Total",
        "Women",
        "Men"
      )
    )
  }

  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task=="data_covid19_daily_report") %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}

#* Lab data
#* @param location_code location_code ("norge")
#* @param lang nb or en
#* @param api_key api_key
#* @get /hc_lab_pos_neg_by_time
#* @serializer highcharts
function(req, res, api_key, lang="nb", location_code){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl("data_covid19_lab_by_time") %>%
    dplyr::filter(granularity_time == "day") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(date, n_neg, n_pos, pr100_pos) %>%
    dplyr::collect()
  setDT(d)

  if(lang=="nb"){
    setnames(d, c(
      "Dato",
      "Negative",
      "Positive",
      "Andel"
    ))
  } else {
    setnames(d, c(
      "Date",
      "Negative",
      "Positive",
      "Percent"
    ))
  }

  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task=="data_covid19_daily_report") %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}












#* These are the locations and location names
#* @param location_code location_code ("norge")
#* @param lang nb or en
#* @param api_key api_key
#* @get /hc_msis_cases_by_time_infected_location
#* @serializer highcharts
function(req, res, api_key, lang="nb", location_code){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  if(lang == "nb"){
    labs_main <- c(
      "Norge",
      "Utlandet",
      "Ukjent"
    )
    labs_week <- "Ukenr"
  } else {
    labs_main <- c(
      "Norway",
      "Abroad",
      "Unknown"
    )
    labs_week <- "Week number"
  }

  d <- pool %>% dplyr::tbl("data_covid19_msis_by_time_infected_abroad") %>%
    dplyr::filter(granularity_time == "week") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(yrwk, n, tag_location_infected) %>%
    dplyr::collect()
  setDT(d)
  d[, tag_location_infected := factor(
    tag_location_infected,
    levels = c(
      "norge",
      "utlandet",
      "ukjent"
    ),
    labels = labs_main
  )]
  d <- dcast.data.table(d, yrwk ~ tag_location_infected, value.var = "n")
  setnames(d, "yrwk", labs_week)

  list(
    last_modified = "2020-04-27 23:21:32",
    data = d
  )
}


#* These are the locations and location names
#* @param location_code location code ("norge" is a common choice)
#* @param api_key api_key
#* @get /hc_reported_cases
#* @serializer highcharts
function(req, res, api_key, location_code){
  stopifnot(location_code %in% c("norge"))
  d <- pool %>% dplyr::tbl("data_covid19_msis_by_time_location") %>%
    dplyr::filter(granularity_time == "day") %>%
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

  list(
    last_modified = "2020-04-27 23:21:32",
    data = d
  )
}





#* These are the locations and location names
#* @param location_code location_code ("norge")
#* @param api_key api_key
#* @get /hc_msis_cases_by_age
#* @serializer highcharts
function(req, res, api_key, location_code){
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl("data_covid19_msis_by_sex_age") %>%
    dplyr::filter(granularity_time == "total") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(age, n) %>%
    dplyr::group_by(age) %>%
    dplyr::summarize(n=sum(n)) %>%
    dplyr::collect()
  setDT(d)

  x_pop <- fhidata::norway_population_b2020[
    year==2020 & level=="nation",
    .(
      pop=sum(pop)
    ),keyby=.(
      age
    )]
  x_pop[,age := fancycut::fancycut(
    age,
    "0-9" = "[0,9]",
    "10-19" = "[10,19]",
    "20-29" = "[20,29]",
    "30-39" = "[30,39]",
    "40-49" = "[40,49]",
    "50-59" = "[50,59]",
    "60-69" = "[60,69]",
    "70-79" = "[70,79]",
    "80-89" = "[80,89]",
    "90+" = "[90,9999]"
  )]
  x_pop <- x_pop[
    ,
    .(
      pop=sum(pop)
    ),keyby=.(
      age
    )]
  d[
    x_pop,
    on="age",
    incidence := round(100000*n/pop,1)
  ]
  d[,n:=NULL]

  setnames(d, c("Aldersgruppe", "Tilfeller per 100 000 innbyggere"))

  list(
    last_modified = "2020-04-27 23:21:32",
    data = d
  )
}

#* These are the locations and location names
#* @param location_code location_code ("norge")
#* @param api_key api_key
#* @get /hc_msis_cases_by_sex
#* @serializer highcharts
function(req, res, api_key, location_code){
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl("data_covid19_msis_by_sex_age") %>%
    dplyr::filter(granularity_time == "total") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(sex, n) %>%
    dplyr::group_by(sex) %>%
    dplyr::summarize(n=sum(n)) %>%
    dplyr::collect()
  setDT(d)

  d[, sex := dplyr::case_when(
    sex == "male" ~ "Menn",
    sex == "female" ~ "Kvinner"
  )]


  setnames(
    d,
    c(
      glue::glue("Kj{fhi::nb$oe}nn"),
      "Antall"
    )
  )

  list(
    last_modified = "2020-04-27 23:21:32",
    data = d
  )
}

#* These are the locations and location names
#* @param location_code location_code ("norge")
#* @param api_key api_key
#* @get /hc_norsyss_comparison_r27_r991_r74_r80_by_time
#* @serializer highcharts
function(req, res, api_key, location_code){
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl("data_norsyss") %>%
    dplyr::filter(tag_outcome %in% c(
      "covid19_vk_ote",
      "engstelig_luftveissykdom_ika_vk_ote",
      "influensa_vk_ote",
      "rxx_for_covid19_vk_ote",
      "akkut_ovre_luftveisinfeksjon_vk_ote"
    )) %>%
    dplyr::filter(granularity_time == "day") %>%
    dplyr::filter(date >= "2020-03-06") %>%
    dplyr::filter(age == "total") %>%
    dplyr::filter(location_code == !!location_code) %>%
    dplyr::collect()
  setDT(d)
  d[,date:=as.Date(date)]

  d[, prop := round(100*n/consult_with_influenza,1)]

  d <- dcast.data.table(d, date ~ tag_outcome, value.var="prop")
  setnames(
    d,
    c(
      "Konsultasjonsdato",
      glue::glue("Akutt {fhi::nb$oe}vre luftveisinfeksjon (R74)"),
      "Covid-19 (mistenkt eller bekreftet, R991)",
      "Engstelig luftveissykdom IKA (R27)",
      "Influensa (R80)",
      "Luftvei diagnosekoder (samlet)"
    )
  )

  list(
    last_modified = "2020-04-27 23:21:32",
    data = d
  )
}

#* These are the locations and location names
#* @param location_code location_code
#* @param api_key api_key
#* @get /hc_icu_by_time
#* @serializer highcharts
function(req, res, api_key, location_code){
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl("data_covid19_nir_by_time") %>%
    dplyr::filter(granularity_time == "day") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(date, n_icu) %>%
    dplyr::collect()
  setDT(d)

  setnames(
    d,
    c(
      "Dato",
      "Antall"
    )
  )

  list(
    last_modified = "2020-04-27 23:21:32",
    data = d
  )
}
