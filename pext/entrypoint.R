library(plumber)

highcharts <- function(){
  function(val, req, res, errorHandler){
    tryCatch({
      j1 <- jsonlite::toJSON(names(val), dataframe = c("values"))
      j2 <- jsonlite::toJSON(val, dataframe = c("values"))
      j2 <- stringr::str_remove(j2,"\\[")

      j <- paste0("[",j1,",",j2)

      res$setHeader("Content-Type", "application/json")
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
