## WHEN DEBUGGING, START RUNNING HERE
## THIS LOADS UP YOUR DATABASE

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


## WHEN DEBUGGING, STOP RUNNING HERE


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

#* key numbers ----
#* @param location_code location code ("norge" is a common choice)
#* @param lang nb or en
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /hc_key_numbers
function(req, res, api_key, prelim=FALSE, lang="nb", location_code="norge"){
  stopifnot(prelim %in% c(T,F))
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  # cum num cases
  val <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_msis_by_time_location",
      "fhino_api_covid19_production_data_autoc19_msis_by_time_location"
    )) %>%
    mandatory_db_filter(
      granularity_time = "day",
      granularity_geo = NULL,
      age = "total",
      sex = "total"
    ) %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::summarize(n=sum(n_pr)) %>%
    dplyr::collect()
  cum_n_msis <- val$n
  cum_n_msis

  # cum num hospital and icu
  val <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_hospital_by_time_location",
      "fhino_api_covid19_production_data_autoc19_hospital_by_time_location"
    )) %>%
    mandatory_db_filter(
      granularity_time = "day",
      granularity_geo = NULL,
      age = "total",
      sex = "total"
    ) %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::summarize(
      cum_n_icu = sum(n_icu),
      cum_n_hospital_main_cause = sum(n_hospital_main_cause)
    ) %>%
    dplyr::collect()
  setDT(val)

  cum_n_hospital_main_cause <- val$cum_n_hospital_main_cause
  cum_n_icu <- val$cum_n_icu
  cum_n_hospital_main_cause
  cum_n_icu

  n_lab <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_lab_by_time",
      "fhino_api_covid19_production_data_autoc19_lab_by_time"
    )) %>%
    mandatory_db_filter(
      granularity_time = "day",
      granularity_geo = NULL,
      age = "total",
      sex = "total"
    ) %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::summarize(n = sum(n_tested)) %>%
    dplyr::collect()
  n_lab <- n_lab$n
  n_lab

  val <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_death_by_time_location",
      "fhino_api_covid19_production_data_autoc19_death_by_time_location"
    )) %>%
    mandatory_db_filter(
      granularity_time = "total",
      granularity_geo = NULL,
      age = "total",
      sex = "total"
    ) %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::collect()
  setDT(val)
  cum_n_deaths <- val$n
  cum_n_deaths

  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task==!!ifelse(prelim,"fhino_api_covid19_copy_database_table_control","fhino_api_covid19_copy_database_table_production")) %>%
    dplyr::select("datetime") %>%
    dplyr::collect()
  last_mod <- last_mod$datetime
  last_mod <- format.Date(last_mod, "%d/%m/%Y")

  if(lang == "nb"){
    retval <- list(
      figures = rbind(
        data.frame(
          key = "cum_n_msis",
          number = cum_n_msis,
          description = "Meldte tilfeller",
          updated = last_mod
        ),

        data.frame(
          key = "cum_n_hospital_any_cause",
          number = cum_n_hospital_main_cause,
          description = "Innlagt sykehus",
          updated = last_mod
        ),

        data.frame(
          key = "cum_n_icu",
          number = cum_n_icu,
          description = "Innlagt intensiv",
          updated = last_mod
        ),

        data.frame(
          key = "cum_n_deaths",
          number = cum_n_deaths,
          description = glue::glue("D{fhi::nb$oe}de"),
          updated = last_mod
        ),

        data.frame(
          key = "n_lab",
          number = n_lab,
          description = "Testet",
          updated = last_mod
        )
      )
    )
  } else {
    retval <- list(
      figures = rbind(
        data.frame(
          key = "cum_n_msis",
          number = cum_n_msis,
          description = "Reported cases",
          updated = last_mod
        ),

        data.frame(
          key = "cum_n_hospital_any_cause",
          number = cum_n_hospital_main_cause,
          description = "Admitted to hospital",
          updated = last_mod
        ),

        data.frame(
          key = "cum_n_icu",
          number = cum_n_icu,
          description = "Admitted to ICU",
          updated = last_mod
        ),

        data.frame(
          key = "cum_n_deaths",
          number = cum_n_deaths,
          description = "Deaths",
          updated = last_mod
        ),

        data.frame(
          key = "n_lab",
          number = n_lab,
          description = "Tested",
          updated = last_mod
        )
      )
    )
  }
}

#* dynamic text ----
#* @param location_code location code ("norge" is a common choice)
#* @param lang nb or en
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /hc_dynamic_text
function(req, res, api_key, prelim=FALSE, lang="nb", location_code="norge"){
  stopifnot(prelim %in% c(T,F))
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "prelim_data_covid19_dynamic_text",
      "data_covid19_dynamic_text"
    )) %>%
     dplyr::select(tag_outcome,value) %>%
     dplyr::collect()
  setDT(d)

  death_average_age <- d[tag_outcome=="death_average_age", value]
  death_media_age   <- d[tag_outcome=="death_media_age", value]
  death_pr100_male  <- d[tag_outcome=="death_pr100_male", value]

  hospital_main_cause_average_age <- d[tag_outcome=="hospital_main_cause_average_age", value]
  hospital_main_cause_pr100_male <- d[tag_outcome=="hospital_main_cause_pr100_male", value]

  icu_main_cause_pr100   <- d[tag_outcome=="icu_main_cause_pr100", value]

  msis_median_age  <- d[tag_outcome=="msis_median_age", value]
  msis_n        <- d[tag_outcome=="msis_n", value]
  msis_n_female   <- d[tag_outcome=="msis_n_female", value]
  msis_n_male    <- d[tag_outcome=="msis_n_male", value]
  msis_pr100_male <- d[tag_outcome=="msis_pr100_male", value]
  n_death     <- d[tag_outcome=="n_death", value]
  n_hospital_main_cause <- d[tag_outcome=="n_hospital_main_cause", value]
  n_icu_main_cause <- d[tag_outcome=="n_icu_main_cause", value]
  n_icu_main_cause_still_in_icu<- d[tag_outcome=="n_icu_main_cause_still_in_icu", value]



  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task==!!ifelse(prelim,"prelim_data_covid19_daily_report_weekday","data_covid19_daily_report_weekday")) %>%
    dplyr::select("date") %>%
    dplyr::collect()
  last_mod <- last_mod$date
  last_mod <- format.Date(last_mod, "%d/%m/%Y")

  if(lang == "nb"){
    retval <- list(
      figures = rbind(
        data.frame(
          key = "msis_n",
          number = msis_n,
          description = "total msis",
          updated = last_mod
        ),

        data.frame(
          key = "msis_pr100_male",
          number = msis_pr100_male ,
          description = "% male msis",
          updated = last_mod
        ),

        data.frame(
          key = "msis_median_age",
          number = msis_median_age ,
          description = "media age msis",
          updated = last_mod
        ),
        data.frame(
          key = "n_hospital_main_cause",
          number = n_hospital_main_cause,
          description = "Total hospitaled Covid-19 as main cause",
          updated = last_mod
        ),
        data.frame(
          key = "hospital_main_cause_pr100_male",
          number = hospital_main_cause_pr100_male,
          description = "Proportion of male in hospitaled Covid-19 as main cause",
          updated = last_mod
        ),
        data.frame(
          key = "hospital_main_cause_average_age",
          number = hospital_main_cause_average_age,
          description = "Mean age hospitaled Covid-19 as main causeaverage_age",
          updated = last_mod
        ),

        data.frame(
          key = "n_icu_main_cause",
          number = n_icu_main_cause,
          description = "Total in icu Covid-19 as main cause",
          updated = last_mod
        ),
        data.frame(
          key = "icu_main_cause_pr100",
          number = icu_main_cause_pr100,
          description = "Proportion of male in icu Covid-19 as main cause",
          updated = last_mod
        ),
        data.frame(
          key = "n_icu_main_cause_still_in_icu",
          number = n_icu_main_cause_still_in_icu,
          description = "Total still in icu Covid-19 as main cause",
          updated = last_mod
        ),
        data.frame(
          key = "n_death",
          number = n_death,
          description = "Total death",
          updated = last_mod
        ),
        data.frame(
          key = "death_pr100_male",
          number = death_pr100_male,
          description = "Proportion of males among death",
          updated = last_mod
        ),
        data.frame(
          key = "death_media_age",
          number = death_media_age ,
          description = "Median age death ",
          updated = last_mod
        ),
        data.frame(
          key = "death_average_age",
          number = death_average_age,
          description = "Mean age death ",
          updated = last_mod
        )
      )
    )
  } else {
    retval <- list(
      figures = rbind(
        data.frame(
          key = "msis_n",
          number = msis_n,
          description = "total msis",
          updated = last_mod
        ),

        data.frame(
          key = "msis_pr100_male",
          number = msis_pr100_male ,
          description = "% male msis",
          updated = last_mod
        ),

        data.frame(
          key = "msis_median_age",
          number = msis_median_age ,
          description = "media age msis",
          updated = last_mod
        ),
        data.frame(
          key = "n_hospital_main_cause",
          number = n_hospital_main_cause,
          description = "Total hospitaled Covid-19 as main cause",
          updated = last_mod
        ),
        data.frame(
          key = "hospital_main_cause_pr100_male",
          number = hospital_main_cause_pr100_male,
          description = "Proportion of male in hospitaled Covid-19 as main cause",
          updated = last_mod
        ),
        data.frame(
          key = "hospital_main_cause_average_age",
          number = hospital_main_cause_average_age,
          description = "Mean age hospitaled Covid-19 as main causeaverage_age",
          updated = last_mod
        ),

        data.frame(
          key = "icu_main_cause",
          number = icu_main_cause,
          description = "Total in icu Covid-19 as main cause",
          updated = last_mod
        ),
        data.frame(
          key = "icu_main_cause_pr100",
          number = icu_main_cause_pr100,
          description = "Proportion of male in icu Covid-19 as main cause",
          updated = last_mod
        ),
        data.frame(
          key = "n_icu_main_cause_still_in_icu",
          number = n_icu_main_cause_still_in_icu,
          description = "Total still in icu Covid-19 as main cause",
          updated = last_mod
        ),
        data.frame(
          key = "n_death",
          number = n_death,
          description = "Total death",
          updated = last_mod
        ),
        data.frame(
          key = "death_pr100_male",
          number = death_pr100_male,
          description = "Proportion of males among death",
          updated = last_mod
        ),
        data.frame(
          key = "death_media_age",
          number = death_media_age ,
          description = "Median age death ",
          updated = last_mod
        ),
        data.frame(
          key = "death_average_age",
          number = death_average_age,
          description = "Mean age death ",
          updated = last_mod
        )
      )
    )
  }
}




#* (B) hc_lab_pos_neg_by_time ----
#* Lab data
#* @param location_code location_code ("norge")
#* @param lang nb or en
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /hc_lab_pos_neg_by_time
#* @serializer highcharts
function(req, res, api_key, prelim=FALSE, lang="nb", location_code){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_lab_by_time",
      "fhino_api_covid19_production_data_autoc19_lab_by_time"

    )) %>%
    mandatory_db_filter(
      granularity_time = "day",
      granularity_geo = NULL,
      age = "total",
      sex = "total"
    ) %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(date, n_neg, n_pos) %>%
    dplyr::collect()
  setDT(d)

  d[,pr100_pos := round(100*n_pos/(n_pos+n_neg),1)]

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
    dplyr::filter(task==!!ifelse(prelim,"fhino_api_covid19_copy_database_table_control","fhino_api_covid19_copy_database_table_production")) %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}

#* (C) hc_msis_cases_by_time_location ----
#* @param location_code location code ("norge" is a common choice)
#* @param granularity_time day or week
#* @param lang nb or en
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /hc_msis_cases_by_time_location
#* @serializer highcharts
function(req, res, api_key, prelim=F, lang="nb", granularity_time, location_code){
  stopifnot(prelim %in% c(T,F))
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(granularity_time %in% c("day","week"))

  valid_locations <- unique(fhidata::norway_locations_b2020$county_code)
  valid_locations <- stringr::str_remove(valid_locations, "county")
  valid_locations <- c("norge", valid_locations)
  stopifnot(location_code %in% valid_locations)

  if(stringr::str_length(location_code)==2) location_code <- paste0("county",location_code)

  d <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_msis_by_time_location",
      "fhino_api_covid19_production_data_autoc19_msis_by_time_location"
    )) %>%
    mandatory_db_filter(
      granularity_time = granularity_time,
      granularity_geo = NULL,
      age = "total",
      sex = "total"
    ) %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(yrwk, date, n_pr) %>%
    dplyr::collect()
  setDT(d)
  d[,date:=as.Date(date)]
  setorder(d, date)

  d[,cum_n_pr := cumsum(n_pr)]

  setcolorder(d,c("yrwk", "date","cum_n_pr","n_pr"))
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
    dplyr::filter(task==!!ifelse(prelim,"fhino_api_covid19_copy_database_table_control","fhino_api_covid19_copy_database_table_production")) %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}

#* (D) hc_msis_cases_by_time_age_sex ----
#* These are the locations and location names
#* @param v version
#* @param yrwk yrwk, or "total"
#* @param location_code location_code ("norge")
#* @param lang nb or en
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /hc_msis_cases_by_time_age_sex
#* @serializer highcharts
function(req, res, api_key, prelim=FALSE, lang="nb", location_code="norge", yrwk="total", v=1){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  if(yrwk == "total"){
    d <- pool %>% dplyr::tbl(
      ifelse(
        prelim,
        "fhino_api_covid19_control_data_autoc19_msis_by_time_sex_age_API",
        "fhino_api_covid19_production_data_autoc19_msis_by_time_sex_age_API"
      )) %>%
      mandatory_db_filter(
        granularity_time = "total",
        granularity_geo = NULL,
        age_not = "total",
        sex_not = "total",
      ) %>%
      dplyr::filter(location_code== !!location_code) %>%
      dplyr::select(age, sex, n) %>%
      dplyr::collect()

  } else {
    d <- pool %>% dplyr::tbl(
      ifelse(
        prelim,
        "fhino_api_covid19_control_data_autoc19_msis_by_time_sex_age_API",
        "fhino_api_covid19_production_data_autoc19_msis_by_time_sex_age_API"
      )) %>%
      mandatory_db_filter(
        granularity_time = "week",
        granularity_geo = NULL,
        age_not = "total",
        sex_not = "total"
      ) %>%
      dplyr::filter(yrwk == !!yrwk) %>%
      dplyr::filter(location_code== !!location_code) %>%
      dplyr::select(age, sex, n) %>%
      dplyr::collect()
  }
  setDT(d)

  available_time <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_msis_by_time_sex_age",
      "fhino_api_covid19_production_data_autoc19_msis_by_time_sex_age"
    )) %>%
    mandatory_db_filter(
      granularity_time = "week",
      granularity_geo = NULL,
      age = NULL,
      sex = NULL
    ) %>%
    dplyr::filter(granularity_time == "week") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::distinct(yrwk) %>%
    dplyr::collect()
  available_time <- available_time$yrwk
  available_time <- c("total",rev(sort(available_time)))

  d <- dcast.data.table(d, age ~ sex, value.var = "n")
  setnames(
    d,
    c(
      "Alder",
      "Kvinner",
      "Menn"
    )
  )

  if(lang=="en"){
    setnames(
      d,
      c(
        "Age",
        "Women",
        "Men"
      )
    )
  }


  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task==!!ifelse(prelim,"fhino_api_covid19_copy_database_table_control","fhino_api_covid19_copy_database_table_production")) %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    available_time = available_time,
    last_modified = last_mod$datetime,
    data = d
  )
}

#* (E/F) hc_msis_cases_map -----
#* Map of MSIS incidence
#* @param measure "n" or "pr100000"
#* @param granularity_geo county or municip
#* @param lang nb or en
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /hc_msis_cases_map
#* @serializer highcharts
function(req, res, api_key, prelim=FALSE, lang="nb", granularity_geo="county", measure="n"){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(granularity_geo %in% c("county", "municip"))
  stopifnot(measure %in% c("n","pr100000"))

  d <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_msis_by_time_location",
      "fhino_api_covid19_production_data_autoc19_msis_by_time_location"
    )) %>%
    mandatory_db_filter(
      granularity_time = "week",
      granularity_geo = granularity_geo,
      age = "total",
      sex = "total"
    ) %>%
    dplyr::group_by(location_code) %>%
    dplyr::summarize(n=sum(n_pr)) %>%
    dplyr::collect()
  setDT(d)

  setorder(d,-n)

  if(measure=="pr100000"){
    x_pop <- fhidata::norway_population_b2020[
      year==2020,
      .(pop=sum(pop)),
      keyby=.(location_code)
      ]
    d[
      x_pop,
      on="location_code",
      pop:=pop
      ]
    d[,n:=round(100000*n/pop,1)]
    d[,pop:=NULL]
  }

  if(granularity_geo=="county"){
    d[
      fhidata::norway_locations_long_b2020,
      on="location_code",
      location_name := location_name
      ]

    d[, location_name := stringr::str_to_lower(location_name)]

    d <- d[,.(
      Fylke = location_name,
      Antall = n
    )]
  } else if(granularity_geo=="municip"){
    d[, Kommunenr := stringr::str_remove(location_code,"municip")]

    d <- d[,.(
      Kommunenr = Kommunenr,
      Antall = n
    )]
  }

  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task==!!ifelse(prelim,"fhino_api_covid19_copy_database_table_control","fhino_api_covid19_copy_database_table_production")) %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}

#* (G) hc_hospital_by_time_location ----
#* @param location_code location code ("norge" is a common choice)
#* @param granularity_time day or week
#* @param lang nb or en
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /hc_hospital_by_time_location
#* @serializer highcharts
function(req, res, api_key, prelim=F, lang="nb", granularity_time="day", location_code="norge"){
  stopifnot(prelim %in% c(T,F))
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(granularity_time %in% "day")
  stopifnot(location_code %in% "norge")

  d <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_hospital_by_time_location",
      "fhino_api_covid19_production_data_autoc19_hospital_by_time_location"
    )) %>%
    mandatory_db_filter(
      granularity_time = granularity_time,
      granularity_geo = NULL,
      age = "total",
      sex = "total"
    ) %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(date, n_hospital_main_cause) %>%
    dplyr::collect()
  setDT(d)
  d[,date:=as.Date(date)]
  setorder(d, date)

  d[,cum_n := cumsum(n_hospital_main_cause)]

  setcolorder(d,c("date","cum_n","n_hospital_main_cause"))

  if(lang=="nb"){
    setnames(d, c(glue::glue("Dato"), "Kumulativt antall", "Nye sykehusinnlegelser"))
  } else {
    setnames(d, c(glue::glue("Date"), "Cumulative total", "New hospital admissions"))
  }


  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task==!!ifelse(prelim,"fhino_api_covid19_copy_database_table_control","fhino_api_covid19_copy_database_table_production")) %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}

#* (I) hc_icu_by_time_location ----
#* @param location_code location code ("norge" is a common choice)
#* @param granularity_time day or week
#* @param lang nb or en
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /hc_icu_by_time_location
#* @serializer highcharts
function(req, res, api_key, prelim=FALSE, lang="nb", granularity_time="day", location_code="norge"){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(granularity_time %in% "day")
  stopifnot(location_code %in% "norge")

  d <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_hospital_by_time_location",
      "fhino_api_covid19_production_data_autoc19_hospital_by_time_location"
    )) %>%
    mandatory_db_filter(
      granularity_time = granularity_time,
      granularity_geo = NULL,
      age = "total",
      sex = "total"
    ) %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(date, n_icu) %>%
    dplyr::collect()
  setDT(d)
  d[,date:=as.Date(date)]
  setorder(d, date)

  d[,cum_n := cumsum(n_icu)]

  setcolorder(d,c("date","cum_n","n_icu"))

  if(lang=="nb"){
    setnames(d, c(glue::glue("Dato"), "Kumulativt antall", "Nye intensivinnleggelser"))
  } else {
    setnames(d, c(glue::glue("Date"), "Cumulative intensive total", "New intensive care admissions"))
  }


  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task==!!ifelse(prelim,"fhino_api_covid19_copy_database_table_control","fhino_api_covid19_copy_database_table_production")) %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}



#* (K) hc_deaths_by_age_sex ----
#* These are the locations and location names
#* @param location_code location_code ("norge")
#* @param lang nb or en
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /hc_deaths_by_age_sex
#* @serializer highcharts
function(req, res, api_key, prelim=FALSE, lang="nb", location_code="norge"){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_demographics",
      "fhino_api_covid19_production_data_autoc19_demographics"
    )) %>%
    mandatory_db_filter(
      granularity_time = "total",
      granularity_geo = NULL,
      age_not = "total",
      sex_not = "total"
    ) %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::filter(tag_outcome == "death") %>%
    dplyr::select(age, sex, n) %>%
    dplyr::collect()

  setDT(d)

  d <- dcast.data.table(d, age ~ sex, value.var = "n")
  setnames(
    d,
    c(
      "Alder",
      "Kvinner",
      "Menn"
    )
  )

  if(lang=="en"){
    setnames(
      d,
      c(
        "Age",
        "Women",
        "Men"
      )
    )
  }


  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task==!!ifelse(prelim,"fhino_api_covid19_copy_database_table_control","fhino_api_covid19_copy_database_table_production")) %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}

#* (M) hc_sysvak_by_time_location ----
#* @param location_code location code ("norge" is a common choice)
#* @param granularity_time day or week
#* @param lang nb or en
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /hc_sysvak_by_time_location
#* @serializer highcharts
function(req, res, api_key, prelim=F, lang="nb", granularity_time, location_code){
  stopifnot(prelim %in% c(T,F))
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(granularity_time %in% c("day","week"))

  valid_locations <- unique(fhidata::norway_locations_b2020$county_code)
  valid_locations <- stringr::str_remove(valid_locations, "county")
  valid_locations <- c("norge", valid_locations)
  stopifnot(location_code %in% valid_locations)

  if(stringr::str_length(location_code)==2) location_code <- paste0("county",location_code)

  d <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_vaccination_time_location",
      "fhino_api_covid19_production_data_autoc19_vaccination_time_location"
    )) %>%
    mandatory_db_filter(
      granularity_time = granularity_time,
      granularity_geo = NULL,
      age = "total",
      sex = "total"
    ) %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(yrwk, date, n, n_dose) %>%
    dplyr::collect()
  setDT(d)
  d[,date:=as.Date(date)]
  setorder(d, date)

  d[n_dose=="partly", n_dose:="forste"]
  d[n_dose=="full", n_dose:="andre"]

  d <- dcast.data.table(
    d,
    yrwk + date ~ n_dose,
    value.var = "n"
  )
  d[, cum_forste := cumsum(forste)]
  d[, cum_andre := cumsum(andre)]

  setcolorder(d,c("yrwk", "date","cum_forste","cum_andre","forste","andre"))
  if(granularity_time=="day"){
    d[, yrwk:=NULL]

    if(lang=="nb"){
      setnames(d, c(
        glue::glue("Dato"),
        "Kumulativt antall personer vaksinert med 1.dose",
        "Kumulativt antall personer vaksinert med 2.dose",
        "Antall personer vaksinert med 1.dose",
        "Antall personer vaksinert med 2.dose"
      ))
    } else {
      setnames(d, c(
        glue::glue("Dato"),
        "Cumulative number of people vaccinated with first dose",
        "Cumulative number of people vaccinated with second dose",
        "Number of people vaccinated with first dose",
        "Number of people vaccinated with second dose"
      ))
    }
  } else {
    d[, date:=NULL]

    if(lang=="nb"){
      setnames(d, c(
        glue::glue("Uke"),
        "Kumulativt antall personer vaksinert med 1.dose",
        "Kumulativt antall personer vaksinert med 2.dose",
        "Antall personer vaksinert med 1.dose",
        "Antall personer vaksinert med 2.dose"
      ))
    } else {
      setnames(d, c(
        glue::glue("Uke"),
        "Cumulative number of people vaccinated with first dose",
        "Cumulative number of people vaccinated with second dose",
        "Number of people vaccinated with first dose",
        "Number of people vaccinated with second dose"
      ))
    }
  }

  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task==!!ifelse(prelim,"fhino_api_covid19_copy_database_table_control","fhino_api_covid19_copy_database_table_production")) %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}

#* (R) hc_sysvak_by_age_sex_location ----
#* @param location_code location code ("norge" is a common choice)
#* @param lang nb or en
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /hc_sysvak_by_age_sex_location
#* @serializer highcharts
function(req, res, api_key, prelim=F, lang="nb", location_code){
  stopifnot(prelim %in% c(T,F))
  stopifnot(lang %in% c("nb", "en"))

  valid_locations <- unique(fhidata::norway_locations_b2020$county_code)
  valid_locations <- stringr::str_remove(valid_locations, "county")
  valid_locations <- c("norge", valid_locations)

  stopifnot(location_code %in% valid_locations)

  if(stringr::str_length(location_code)==2) location_code <- paste0("county",location_code)

  d <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_vaccination_by_sex_age_location",
      "fhino_api_covid19_production_data_autoc19_vaccination_by_sex_age_location"
    )) %>%
    mandatory_db_filter(
      granularity_time = "total",
      granularity_geo = NULL,
      age_not = "total",
      sex_not = "total"
    ) %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(sex, age, n, n_dose) %>%
    dplyr::collect()
  setDT(d)
  d <-d[age!="0-15"]
  d[n_dose=="partly", n_dose:="1.dose"]
  d[n_dose=="full", n_dose:="2.dose"]

  d <- dcast.data.table(
    d,
    age~n_dose+sex,
    value.var = c("n")
  )

  d[,age:=gsub("-", "\\1 - \\2", age)]

  setcolorder(d, c("age",
                   "1.dose_female",
                   "2.dose_female",
                   "1.dose_male",
                   "2.dose_male"))
  setnames(
    d,
    c(
      "Aldersgrupper",
      "Kvinner 1.dose",
      "Kvinner 2.dose",
      "Menn 1.dose",
      "Menn 2.dose"
    )
  )

  if(lang=="en"){
    setnames(
      d,
      c(
        "Age group",
        "Women first dose",
        "Women second dose",
        "Men first dose",
        "Men second dose"
      )
    )
  }

  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task==!!ifelse(prelim,"fhino_api_covid19_copy_database_table_control","fhino_api_covid19_copy_database_table_production")) %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}



#* (N/O/P/Q) hc_sysvak_map -----
#* Map of MSIS incidence
#* @param dose_number 1 or 2
#* @param granularity_geo county or municip
#* @param lang nb or en
#* @param prelim TRUE or FALSE
#* @param api_key api_key
#* @get /hc_sysvak_map
#* @serializer highcharts
function(req, res, api_key, prelim=FALSE, lang="nb", granularity_geo="county", dose_number = 1){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(granularity_geo %in% c("county", "municip"))
  stopifnot(dose_number %in% c(1, 2))

  if(dose_number==1){
    dose_number <- "partly"
  } else {
    dose_number <- "full"
  }

  d <- pool %>% dplyr::tbl(
    ifelse(
      prelim,
      "fhino_api_covid19_control_data_autoc19_vaccination_time_location",
      "fhino_api_covid19_production_data_autoc19_vaccination_time_location"
    )) %>%
    mandatory_db_filter(
      granularity_time = "total",
      granularity_geo = granularity_geo,
      age = "total",
      sex = "total"
    ) %>%
    dplyr::filter(n_dose == !!dose_number) %>%
    dplyr::collect()
  setDT(d)

  setorder(d,-n)

  if(granularity_geo=="county"){
    d[
      fhidata::norway_locations_long_b2020,
      on="location_code",
      location_name := location_name
    ]

    d[, location_name := stringr::str_to_lower(location_name)]

    d <- d[,.(
      Fylke = location_name,
      Antall = n
    )]
  } else if(granularity_geo=="municip"){
    d[, Kommunenr := stringr::str_remove(location_code,"municip")]

    d <- d[,.(
      Kommunenr = Kommunenr,
      Antall = n
    )]
  }

  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task==!!ifelse(prelim,"fhino_api_covid19_copy_database_table_control","fhino_api_covid19_copy_database_table_production")) %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}







#* location_code has a "norge"
#* SELV (1) hc_sr_symptoms_by_time ----
#* These are the locations and location names
#* @param lang nb
#* @param api_key api_key
#* @get /hc_sr_symptoms_by_time
#* @serializer highcharts
function(req, res, api_key, lang="nb", location_code="norge"){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% "norge")


  d <- pool %>% dplyr::tbl("data_covid19_self_reporting") %>%
    dplyr::filter(age == "total") %>%
    dplyr::filter(sex == "total") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(date, n) %>%
    dplyr::collect()
  setDT(d)
  d[,date:=as.Date(date)]
  setorder(d, date)

  setcolorder(d,c("date","n"))
  setnames(d, c(glue::glue("Dato"), "Antall"))

  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task=="data_covid19_self_reporting") %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}


#* SELV (2) hc_sr_symptoms_by_age_sex ----
#* These are the locations and location names
#* @param location_code location_code ("norge")
#* @param lang nb
#* @param api_key api_key
#* @get /hc_sr_symptoms_by_age_sex
#* @serializer highcharts
function(req, res, api_key, lang="nb", location_code="norge"){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl("data_covid19_self_reporting") %>%
    dplyr::filter(age != "total") %>%
    dplyr::filter(sex != "total") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(n,sex,age) %>%
    dplyr::group_by(age, sex) %>%
    dplyr::summarize(n=sum(n)) %>%
    dplyr::collect()
  setDT(d)
  d[,age:=as.numeric(age)]

  d <- dcast.data.table(d, age~sex , value.var = "n")
  d[, total := female + male]
  setnames(
    d,
    c(
      "Alder",
      "Kvinner",
      "Menn",
      "Alle"
    )
  )
  setcolorder(
    d,
    c(
      "Alder",
      "Alle",
      "Menn",
      "Kvinner"
    )
  )



  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task=="data_covid19_daily_report") %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}



#* SELV (3) hc_sr_symptoms_map ----
#* Map of Symptoms incidence
#* @param measure "n" or "pr10000"
#* @param granularity_geo county
#* @param lang nb
#* @param api_key api_key
#* @get /hc_sr_symptoms_map
#* @serializer highcharts
function(req, res, api_key, lang="nb", granularity_geo="county", measure="n"){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(granularity_geo %in% c("county"))
  stopifnot(measure %in% c("n","pr10000"))

  d <- pool %>% dplyr::tbl("data_covid19_self_reporting") %>%
    dplyr::filter(granularity_geo== !!granularity_geo) %>%
    dplyr::filter(age == "total") %>%
    dplyr::filter(sex == "total") %>%
    dplyr::group_by(location_code) %>%
    dplyr::summarize(n=sum(n)) %>%
    dplyr::collect()
  setDT(d)

  setorder(d,-n)

  if(measure=="pr10000"){
    x_pop <- fhidata::norway_population_b2020[
      year==2020,
      .(pop=sum(pop)),
      keyby=.(location_code)
      ]
    d[
      x_pop,
      on="location_code",
      pop:=pop
      ]
    d[,n:=round(10000*n/pop,1)]
    d[,pop:=NULL]
  }


  d[
      fhidata::norway_locations_long_b2020,
      on="location_code",
      location_name := location_name
      ]

    d[, location_name := stringr::str_to_lower(location_name)]

    d <- d[,.(
      Fylke = location_name,
      Antall = n
    )]

  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task=="data_covid19_daily_report") %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}



#* SELV (4) hc_sr_symptoms_with_without_feber ----
#* These are the locations and location names
#* @param location_code location_code ("norge")
#* @param lang nb
#* @param api_key api_key
#* @get /hc_sr_symptoms_with_without_feber
#* @serializer highcharts
function(req, res, api_key, lang="nb", location_code="norge"){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl("data_covid19_self_reporting") %>%
    dplyr::filter(age == "total") %>%
    dplyr::filter(sex == "total") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(
      n_symps_yesfever_cough_or_breath,
      n_symps_nofever_cough_or_breath,
      n_symps_yesfever_muscle_or_headache,
      n_symps_nofever_muscle_or_headache,
      n_symps_yesfever_throat_or_nose,
      n_symps_nofever_throat_or_nose,
      n_symps_yesfever_gastro_or_taste_smell_or_other,
      n_symps_nofever_gastro_or_taste_smell_or_other) %>%
    dplyr::summarize(
      n_symps_yesfever_cough_or_breath=sum(n_symps_yesfever_cough_or_breath),
      n_symps_nofever_cough_or_breath=sum(n_symps_nofever_cough_or_breath),
      n_symps_yesfever_muscle_or_headache=sum(n_symps_yesfever_muscle_or_headache),
      n_symps_nofever_muscle_or_headache=sum(n_symps_nofever_muscle_or_headache),
      n_symps_yesfever_throat_or_nose=sum( n_symps_yesfever_throat_or_nose),
      n_symps_nofever_throat_or_nose=sum( n_symps_nofever_throat_or_nose),
      n_symps_yesfever_gastro_or_taste_smell_or_other=sum(n_symps_yesfever_gastro_or_taste_smell_or_other),
      n_symps_nofever_gastro_or_taste_smell_or_other=sum(n_symps_nofever_gastro_or_taste_smell_or_other)) %>%
    dplyr::collect()
  setDT(d)

  colA = c("n_symps_yesfever_cough_or_breath",
           "n_symps_yesfever_throat_or_nose",
           "n_symps_yesfever_muscle_or_headache",
            "n_symps_yesfever_gastro_or_taste_smell_or_other")
  colB = c("n_symps_nofever_cough_or_breath",
           "n_symps_nofever_throat_or_nose",
           "n_symps_nofever_muscle_or_headache",
           "n_symps_nofever_gastro_or_taste_smell_or_other")

  d = melt(d, measure = list(colA, colB), value.name = c("medfeber", "tenfeber"))

d[variable==1,Symptom:="hoste eller tungpust"]
d[variable==2,Symptom:=glue::glue("s{fhi::nb$aa}r hals eller rennende nese")]
d[variable==3,Symptom:="muskelverk eller hodepine"]
d[variable==4,Symptom:= "andre symptomer"]

d[,variable:=NULL]

setcolorder(d,c("Symptom"))

setnames(
  d,
  c(
    "Symptom",
    "Med feber",
    "Uten feber"
  )
)

  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task=="data_covid19_daily_report") %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}



#* SELV (5) hc_sr_symptoms_shape ----
#* These are the locations and location names
#* @param location_code location_code ("norge")
#* @param lang nb or en
#* @param api_key api_key
#* @get /hc_sr_symptoms_shape
#* @serializer highcharts
function(req, res, api_key, lang="nb", location_code="norge"){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl("data_covid19_self_reporting") %>%
    dplyr::filter(age != "total") %>%
    dplyr::filter(sex != "total") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(
      n_today_0normal,
      n_today_1tired,
      n_today_2need_rest,
      n_today_3bedridden_some_help,
      n_today_4bedridden_lots_help)%>%
    dplyr::summarize(n_today_0normal=sum(n_today_0normal),
                    n_today_1tired=sum(n_today_1tired),
                    n_today_2need_rest=sum(n_today_2need_rest),
                    n_today_3bedridden_some_help=sum(n_today_3bedridden_some_help),
                    n_today_4bedridden_lots_help=sum(n_today_4bedridden_lots_help)) %>%
    dplyr::collect()
  setDT(d)

  colA = c("n_today_0normal",
           "n_today_1tired",
           "n_today_2need_rest",
           "n_today_3bedridden_some_help",
           "n_today_4bedridden_lots_help")

  d = melt(d, measure = list(colA), value.name = c("n"))

  d[variable=="n_today_0normal",Kategori:="Som vanlig"]
  d[variable=="n_today_1tired",Kategori:="Er mer sliten enn vanlig, men er for det meste oppe"]
  d[variable=="n_today_2need_rest",Kategori:="Trenger mye hvile, men er oppe innimellom"]
  d[variable=="n_today_3bedridden_some_help",Kategori:= "Er sengeliggende og trenger noe hjelp"]
  d[variable=="n_today_4bedridden_lots_help",Kategori:= "Er sengeliggende og trenger hjelp til det meste"]

  d[,variable:=NULL]

  setcolorder(d,c("Kategori"))



  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task=="data_covid19_daily_report") %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}


#* SELV (6) hc_sr_symptoms_dr_contact ----
#* These are the locations and location names
#* @param location_code location_code ("norge")
#* @param lang nb
#* @param api_key api_key
#* @get /hc_sr_symptoms_dr_contact
#* @serializer highcharts
function(req, res, api_key, lang="nb", location_code="norge"){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl("data_covid19_self_reporting") %>%
    dplyr::filter(age != "total") %>%
    dplyr::filter(sex != "total") %>%
    dplyr::filter(location_code== !!location_code) %>%
    dplyr::select(sex,n_contact_doctor_yes,n_contact_doctor_no) %>%
    dplyr::group_by(sex) %>%
    dplyr::summarize(
      n_contact_doctor_yes=sum(n_contact_doctor_yes),
      n_contact_doctor_no=sum(n_contact_doctor_no)
      ) %>%
    dplyr::collect()
  setDT(d)

  d[, Sum := n_contact_doctor_yes + n_contact_doctor_no]

  d <- dcast(melt(d, id.vars = "sex"), variable ~ sex)

  d[, Alle := female + male]

  d[variable=="n_contact_doctor_yes", variable:=glue::glue("Har v{fhi::nb$aa}rt i kontakt med lege")]
  d[variable=="n_contact_doctor_no", variable:=glue::glue("Har ikke v{fhi::nb$aa}rt i kontakt med lege")]


  setnames(
    d,
    c(
      "",
      "Kvinner",
      "Menn",
      "Alle"
    )
  )


  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task=="data_covid19_daily_report") %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}












#* hc_msis_cases_by_age_sex ----
#* These are the locations and location names
#* @param v version
#* @param location_code location_code ("norge")
#* @param lang nb or en
#* @param api_key api_key
#* @get /hc_msis_cases_by_age_sex
#* @serializer highcharts
function(req, res, api_key, lang="nb", location_code, v=1){
  stopifnot(lang %in% c("nb", "en"))
  stopifnot(location_code %in% c("norge"))

  d <- pool %>% dplyr::tbl("data_covid19_msis_by_time_sex_age") %>%
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

  if(v==2){
    if(lang=="en"){
      d[,Total:=NULL]
    } else {
      d[,Totalt := NULL]
    }
  }

  last_mod <- pool %>% dplyr::tbl("rundate") %>%
    dplyr::filter(task==!!ifelse(prelim,"prelim_data_covid19_daily_report_weekday","data_covid19_daily_report_weekday")) %>%
    dplyr::select("datetime") %>%
    dplyr::collect()

  list(
    last_modified = last_mod$datetime,
    data = d
  )
}

#* hc_msis_cases_by_time_infected_location ----
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

#* hc_reported_cases ----
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




#* hc_msis_cases_by_age ----
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

#* hc_msis_cases_by_sex ----
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

#* hc_norsyss_comparison_r27_r991_r74_r80_by_time ----
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

#* hc_icu_by_time ----
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

