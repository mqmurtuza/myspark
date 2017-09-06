##############################   Logging   ###################################
OFF   = 0         ## debugging disabled
FATAL = 1         ## least vebose
ERROR = 2         ##      .
INFO  = 3         ##      .
DEBUG = 4         ##      .
TRACE = 5         ## most verbose
LOG_LVL = OFF   ## OVERRIDE THIS IN YOUR SCRIPT
##############################################################################

##############################################################################
## wrapper logging functions to manage log levels
##
## Function:  log_X()
##
## value:  the string being logged
##
## TODO:  make this into a library or replace with one
##############################################################################
log_fatal <- function(value)
      { if(LOG_LVL && LOG_LVL >= FATAL) log(paste("FATAL:",value)) }
log_error <- function(value)
      { if(LOG_LVL && LOG_LVL >= ERROR) log(paste("ERROR:",value)) }
log_info  <- function(value)
      { if(LOG_LVL && LOG_LVL >= INFO) log(paste("INFO: ",value)) }
log_debug <- function(value)
      { if(LOG_LVL && LOG_LVL >= DEBUG) log(paste("DEBUG:",value)) }
log_trace <- function(value)
      { if(LOG_LVL && LOG_LVL >= TRACE) log(paste("TRACE:",value)) }

##############################################################################
##############################################################################
logTS_fatal <- function(value)
{
  if(LOG_LVL && LOG_LVL >= FATAL)
  {
    log("FATAL: time-series")
    print(value)
  }
}

##############################################################################
##############################################################################
logTS_error <- function(value)
{
  if(LOG_LVL && LOG_LVL >= ERROR)
  {
    log("ERROR: time-series")
    print(value)
  }
}

##############################################################################
##############################################################################
logTS_info  <- function(value)
{
  if(LOG_LVL && LOG_LVL >= INFO)
  {
    log("INFO:  time-series")
    print(value)
  }
}

##############################################################################
##############################################################################
logTS_debug <- function(value)
{
  if(LOG_LVL && LOG_LVL >= DEBUG)
  {
    log("DEBUG: time-series")
    print(value)
  }
}

##############################################################################
##############################################################################
logTS_trace <- function(value)
{
  if(LOG_LVL && LOG_LVL >= TRACE)
  {
    log("TRACE: time-series")
    print(value)
  }
}

##############################################################################
## for logging SparkR DataFrames
##############################################################################
logDF_fatal <- function(value, df, nr=DEFAULT_ROWS)
    { if(LOG_LVL && LOG_LVL >= FATAL) logDF(paste("FATAL:",value),df,rows=nr) }
logDF_error <- function(value, df, nr=DEFAULT_ROWS)
    { if(LOG_LVL && LOG_LVL >= ERROR) logDF(paste("ERROR:",value),df,rows=nr) }
logDF_info  <- function(value, df, nr=DEFAULT_ROWS)
    { if(LOG_LVL && LOG_LVL >= INFO) logDF(paste("INFO: ",value),df,rows=nr) }
logDF_debug <- function(value, df, nr=DEFAULT_ROWS)
    { if(LOG_LVL && LOG_LVL >= DEBUG) logDF(paste("DEBUG:",value),df,rows=nr) }
logDF_trace <- function(value, df, nr=DEFAULT_ROWS)
    { if(LOG_LVL && LOG_LVL >= TRACE) logDF(paste("TRACE:",value),df,rows=nr) }

##############################################################################
## for logging R DataFrames (Spark 1.6)
##############################################################################
logRDF_fatal <- function(sqlCtxt, value, df, rnr=DEFAULT_ROWS)
      { logDF_fatal(value,createDataFrame(sqlCtxt,df),nr=rnr) }
logRDF_error <- function(sqlCtxt, value, df, rnr=DEFAULT_ROWS)
      { logDF_error(value,createDataFrame(sqlCtxt,df),nr=rnr) }
logRDF_info  <- function(sqlCtxt, value, df, rnr=DEFAULT_ROWS)
      { logDF_info(value,createDataFrame(sqlCtxt,df),nr=rnr) }
logRDF_debug <- function(sqlCtxt, value, df, rnr=DEFAULT_ROWS)
      { logDF_debug(value,createDataFrame(sqlCtxt,df),nr=rnr) }
logRDF_trace <- function(sqlCtxt, value, df, rnr=DEFAULT_ROWS)
      { logDF_trace(value,createDataFrame(sqlCtxt,df),nr=rnr) }

##############################################################################
## for logging R DataFrames (Spark 2.0)
##############################################################################
logRDF2_fatal <- function(value, df, rnr=DEFAULT_ROWS)
      { logDF_fatal(value,createDataFrame(df),nr=rnr) }
logRDF2_error <- function(value, df, rnr=DEFAULT_ROWS)
      { logDF_error(value,createDataFrame(df),nr=rnr) }
logRDF2_info  <- function(value, df, rnr=DEFAULT_ROWS)
      { logDF_info(value,createDataFrame(df),nr=rnr) }
logRDF2_debug <- function(value, df, rnr=DEFAULT_ROWS)
      { logDF_debug(value,createDataFrame(df),nr=rnr) }
logRDF2_trace <- function(value, df, rnr=DEFAULT_ROWS)
      { logDF_trace(value,createDataFrame(df),nr=rnr) }

##############################################################################
## for logging R Objects
##############################################################################
logObj_fatal <- function(object){if(LOG_LVL && LOG_LVL >= FATAL) print(object)}
logObj_error <- function(object){if(LOG_LVL && LOG_LVL >= ERROR) print(object)}
logObj_info  <- function(object){if(LOG_LVL && LOG_LVL >= INFO) print(object)}
logObj_debug <- function(object){if(LOG_LVL && LOG_LVL >= DEBUG) print(object)}
logObj_trace <- function(object){if(LOG_LVL && LOG_LVL >= TRACE) print(object)}

##############################################################################
## Logging function
##
## Function:  log()
##
## level:  the logging level
## value:  the string being logged
##
## TODO:  make this into a library
##############################################################################
log <- function(value)
{
  if(LOG_LVL) print(paste("[",
                    system("date +%H:%M:%S.%N|sed 's/......$//'",
                    intern=TRUE), "]: ", value, sep=""))
}

##############################################################################
## Log a dataframe function
##
## Function:  logDF()
##
## level:  the logging level
## value:  the string being logged
##
## TODO:  make this into a library
##############################################################################
logDF <- function(value, dframe, rows=20)
{
  if(LOG_LVL)
  {
    log(paste(value,":",sep=""))
    showDF(dframe, numRows=rows)
  }
}
