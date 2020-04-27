library(plumber)

highcharts <- function(){
  function(val, req, res, errorHandler){
    tryCatch({
      if(inherits(val, "list")){
        # take out the date
        last_modified <- val$last_modified

        last_modified_day_name <- c(
          "Mon",
          "Tue",
          "Wed",
          "Thu",
          "Fri",
          "Sat",
          "Sun"
        )[lubridate::wday(last_modified,week_start = 1)]

        last_modified_day <- lubridate::day(last_modified)
        last_modified_month <- c(
          "Jan",
          "Feb",
          "Mar",
          "Apr",
          "May",
          "Jun",
          "Jul",
          "Aug",
          "Sep",
          "Oct",
          "Nov",
          "Dec"
        )[lubridate::month(last_modified)]
        last_modified_year <- lubridate::year(last_modified)

        last_modified_hour <- lubridate::hour(last_modified)
        last_modified_minute <- lubridate::minute(last_modified)
        last_modified_second <- lubridate::second(last_modified)

        last_modified_tz <- format(lubridate::now(tzone = "Europe/Oslo"),"%Z")

        last_modified <- glue::glue(
          "{last_modified_day_name}, {last_modified_day} {last_modified_month} {last_modified_year} {last_modified_hour}:{last_modified_minute}:{last_modified_second} {last_modified_tz}"
        )

        include_last_modified <- TRUE

        # get val out
        val <- val$data
      } else {
        include_last_modified <- FALSE
      }
      j1 <- jsonlite::toJSON(names(val), dataframe = c("values"))
      j2 <- jsonlite::toJSON(val, dataframe = c("values"))
      j2 <- stringr::str_remove(j2,"\\[")

      j <- paste0("[",j1,",",j2)

      res$setHeader("Content-Type", "application/json")
      if(include_last_modified) res$setHeader("Last-Modified", last_modified)
      res$body <- j

      return(res$toResponse())
    }, error=function(e){
      errorHandler(req, res, e)
    })
  }
}
try(plumber::addSerializer("highcharts", highcharts),TRUE)

p1 <- plumber$new("plumber/covid19.R")
p2 <- plumber$new("plumber/covid19_modelling.R")
p3 <- plumber$new("plumber/test.R")
p4 <- plumber$new("plumber/test_authentication.R")

master_p <- plumber$new()
master_p$mount("/covid19", p1)
master_p$mount("/covid19_modelling", p2)
master_p$mount("/test", p3)
master_p$mount("/test_authentication", p4)

master_p$run(port=8000, host="0.0.0.0", swagger = TRUE)
