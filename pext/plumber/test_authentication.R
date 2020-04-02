valid_api_keys <- c("test")

#* @filter auth-user
function(req, username=""){
  req$valid_key <- FALSE

  api_key <- req$args$api_key

  if (is.null(api_key)){
    # No api_key provided
  } else if (api_key %in% valid_api_keys){
    # username is valid
    req$valid_key <- TRUE
  } else {
    # api_key was provided, but invalid
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

#* Authnetication required
#* @param api_key api_key
#* @get /me
function(req, api_key){
  # Safe to assume we have a user, since we've been
  # through all the filters and would have gotten an
  # error earlier if we weren't.
  list(fhidata::norway_locations_long_b2020)
}

#* Authnetication not required
#* @preempt require-auth
#* @get /about
function(){
  list(descript="This is a demo service that uses authentication!")
}


