library(plumber)

p1 <- plumber$new("plumber/covid19_dagsrapport.R")
p2 <- plumber$new("plumber/test.R")
p3 <- plumber$new("plumber/test_authentication.R")

master_p <- plumber$new()
master_p$mount("/covid19_dagsrapport", p1)
master_p$mount("/test", p2)
master_p$mount("/test_authentication", p3)

master_p$run(port=8000, host="0.0.0.0", swagger = TRUE)
