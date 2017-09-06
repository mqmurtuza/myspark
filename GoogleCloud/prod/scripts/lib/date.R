##############################################################################
##
## A collection of functions for working with dates
##
## Library:  date.R 
##
##############################################################################


##############################################################################
## Returns TRUE if the string provided is a valid week in the format YYYYWW
##
## Function:  valid_yyyyww_str()
##
## TODO:  this can be made more robust but it's a start
##############################################################################
valid_yyyyww_str <- function(the_date)
{
#  tryCatch()
  date <- as.integer(the_date)

  if(is.integer(date))
  {
    ## ensure week is between 1 and 52
    if(!((date %% 100) <= 52 && (date %% 100) != 0)) { return(FALSE) }

    ## ensure year is 4 digits
    if(!((as.integer(date / 100) <= 9999) &&
                 (as.integer(date / 100) > 1000))) { return(FALSE) }
  }

  return(TRUE)
}

##############################################################################
## TODO:  make this default to the current calendar week
##############################################################################
week_as_integer <- function(the_week=c(2013, 01))
{
  week_int <- the_week[1]
  week_int <- week_int * 100
  week_int <- week_int + the_week[2]
}

##############################################################################
## Returns the fiscal year and week for the date specified, or for the
## current date if none specified, factoring in the requested offset
##
## Function:  get_fiscal_year_week()
##
## NOTE:  REFERENCE_FISCAL_YR_WK_1 is the calendar fiscal year week
##        corresponding to fiscal year week #1 of that year.  This should be
##        a safe enough distance in the past that it is not part of the
##        historical data set
##
##  NOTE: We are only using this function in PFE to offset the date. And the date is
##        is passed to the function in fiscal format. i.e. is_fiscal=TRUE
##        Currently there are issues when converting calendar date to fiscal date
##        because some years have 53 weeks like (2012, 2017) so boundary conditions
##        for those years do not match.
##        Also currently it rolls over when week is 52. It can be fixed by adding 1 to line 99
##############################################################################
get_fiscal_year_week <- function(date=0, offset=0, is_fiscal=FALSE)
{
  ## DO NOT CHANGE THESE TWO PARAMETERS
  REFERENCE_FISCAL_YR_1 = 2011
  REFERENCE_FISCAL_WK_1 = 6

  ## use the current date or parse out the year and week from the
  ## supplied date
  if(date == 0) {
    year <- system("date +%Y", intern=TRUE)
    week <- system("date +%W", intern=TRUE)
  } else {
    year <- as.integer(as.integer(date) / 100)
    week <- as.integer(date) %% 100
    #is_fiscal=TRUE
  }

  ## convert to fiscal date if we're dealing with a calendar date
  if(!is_fiscal) {
    log_debug(paste("Calendar Year Week: ", year, week, sep=""))

    ## determine number of weeks since reference fiscal week 1
    ## note: the "weeks since fiscal week 1" calculation is inclusive of curr wk
    if(year != REFERENCE_FISCAL_YR_1) {
      wks_since_ref_fiscal_wk1 <- 52 - REFERENCE_FISCAL_WK_1
      wks_since_ref_fiscal_wk1 <- wks_since_ref_fiscal_wk1 + as.integer(week)
      wks_since_ref_fiscal_wk1 <- wks_since_ref_fiscal_wk1 +
                      ((as.integer(year) - REFERENCE_FISCAL_YR_1 - 1) * 52)
    } else {
      wks_since_ref_fiscal_wk1 <- (as.integer(week) - REFERENCE_FISCAL_WK_1)
    }

    ## find out the current fiscal week number
    fiscal_week <- (wks_since_ref_fiscal_wk1 %% 52) + 1

    ## find out the fiscal year
    if((fiscal_week + REFERENCE_FISCAL_WK_1) > 52) {

      ## this condition means the actual calendar rolled over but the fiscal
      ## calendar has not...so roll it back by one year.
      ## Example: when fiscal_week == 50 and REFERENCE_FISCAL_WK_1 == 6,
      ##          then the actual year and the fiscal year are not equal
      fiscal_year <- as.integer(year) - 1
    } else {
      fiscal_year <- as.integer(year)
    }
  } else {
    fiscal_year = as.integer(year)
    fiscal_week = as.integer(week)
  }

  log_debug(paste("Fiscal Year Week: ", fiscal_year,
                                  sprintf("%02d", fiscal_week), sep=""))
  log_debug(paste("Offset to factor in (weeks): ", offset))

  yyyyww <- subtract_weeks((fiscal_year * 100) + fiscal_week, offset)

  delta_year <- as.integer(yyyyww / 100)
  delta_week <- as.integer(yyyyww %% 100)

  log_debug(paste("Calibrated date:", delta_year, ",", delta_week))

  return(c(delta_year, delta_week))
}

##############################################################################
##############################################################################
subtract_weeks <- function(yyyyww, offset)
{

  year <- as.integer(as.integer(yyyyww) / 100)
  week <- as.integer(yyyyww) %% 100

  year_delta <- as.integer(as.integer(offset) / 52)
  week_delta <- as.integer(offset) %% 52

  if(week_delta > week)
  {
    year_delta <- year_delta + 1
    week_delta <- week_delta - week
    week = 52
  }

  new_year <- year - year_delta
  new_week <- week - week_delta

  if(new_week == 0)
  {
    new_year <- new_year - 1
    new_week <- 52
  }

  log_debug(paste("Year calculation:", year, "-", year_delta))
  log_debug(paste("Week calculation:", week, "-", week_delta))

  new_yyyyww <- as.integer((new_year * 100) + new_week)
  log_debug(paste("Returning:", new_yyyyww))

  return(new_yyyyww)
}

##############################################################################
##############################################################################
add_weeks <- function(yyyyww, offset)
{
  
  if(offset > 0) {
    year <- as.integer(as.integer(yyyyww) / 100)
    week <- as.integer(yyyyww) %% 100

    tot_weeks <- week + offset

    year_delta <- as.integer(tot_weeks / 52)
    new_week <- as.integer(tot_weeks %% 52)

    if(new_week == 0)
    {
      new_week <- 52
      year_delta <- year_delta - 1
    }

    new_year <- year + year_delta

    new_yyyyww <- as.integer((new_year * 100) + new_week)
  } else {
    new_yyyyww <- yyyyww
  }

  return(new_yyyyww)
}

##############################################################################
##############################################################################
to_yyyyww_vector <- function(yyyyww)
{
    year <- as.integer(as.integer(yyyyww) / 100)
    week <- as.integer(yyyyww) %% 100
    return(c(year, week))
}

##############################################################################
## this is scary but please don't change the variable name
## from 'p_fiscal_yr_wk_nbr' to 'fiscal_yr_wk_nbr' because it will cause the
## merge() to fail later in the script...apparently it is deriving the name of
## the column in the timeseq DataFrame, from this variable name and when
## that name collides with the name of the column in the DataFrame it is being
## merged with it will fail.  Maybe this will make sense later but for now
## just leave it!
##
## TODO:  Not working for all scenarios...fix it
## TODO:  add optional parameter to specify the name of the column
##############################################################################
date_range <- function(delta=c(2013, 01), weeks=104,
                                          cname="p_fiscal_yr_wk_nbr")
{
  ## initialize
  year_boundaries <- 0  
  e_year = delta[1]
  e_week = 52
  start_str = ""
  end_str = ""

  log_debug(paste("delta[1]:", delta[1]))
  log_debug(paste("delta[2]:", delta[2]))
  log_debug(paste("weeks:", weeks))

  months_to_eoy <- 52 - as.integer(delta[2]) + 1

  if(weeks <= months_to_eoy) {
    final_week = as.integer(delta[2]) + weeks - 1
    final_year = as.integer(delta[1])
  } else {
    year_boundaries <- as.integer(((weeks - months_to_eoy) / 53) + 1)
    final_year <- as.integer(delta[1]) + year_boundaries
    final_week <- (weeks - months_to_eoy) %% 52
    final_week <- ifelse(final_week == 0, 52, final_week)
  }

  ## set start year and week for first sequence
  start_str = paste(as.integer(delta[1]),
                    sprintf("%02d", as.integer(delta[2])), sep="")

  yr = as.integer(delta[1])

  ## now loop through for each year
  for(i in yr:(yr+year_boundaries))
  {
    if(i >= (yr+year_boundaries)) break

    end_str = paste(i, "52", sep="")

    log_debug(paste("Generating Sequence:",start_str,"-",end_str))
    p_fiscal_yr_wk_nbr <- seq(as.integer(start_str), as.integer(end_str), 1)

    if(i == yr) {
      date_frame <- data.frame(p_fiscal_yr_wk_nbr)
    } else {
      tmp_frame <- date_frame
      date_frame <- rbind(tmp_frame, data.frame(p_fiscal_yr_wk_nbr))
    }

    start_str = paste(as.integer(i+1),"01",sep="")
  }

  ## set end year and week for last sequence
  end_str = paste(final_year, sprintf("%02d", final_week), sep="")

  log_debug(paste("Generating Sequence:",start_str,"-",end_str))
  p_fiscal_yr_wk_nbr <- seq(as.integer(start_str), as.integer(end_str), 1)

  if(year_boundaries > 0) {
    tmp_frame <- date_frame
    date_frame <- rbind(tmp_frame, data.frame(p_fiscal_yr_wk_nbr))
  } else {
    date_frame <- data.frame(p_fiscal_yr_wk_nbr)
  }

  colnames(date_frame) <- c(cname)

  return(date_frame)
}
