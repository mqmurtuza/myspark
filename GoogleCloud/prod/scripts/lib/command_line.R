##############################################################################
##############################################################################
parse_cmd_line <- function(cmd_line, key, default="")
{
  search_str = paste("^",key,sep="")

  for(i in 1:length(cmd_line))
  {
    if((length(grep(search_str, cmd_line[i], value=FALSE)) > 0)
        && grep(search_str, cmd_line[i], value=FALSE))
    {
      return(sub(search_str, cmd_line[i], replacement=""))
    }
  }

  return(default)
}

##############################################################################
##############################################################################
cmd_line_parameter_present <- function(cmd_line, key)
{
  search_str = paste("^",key,sep="")

  for(i in 1:length(cmd_line))
  {
    if((length(grep(search_str, cmd_line[i], value=FALSE)) > 0)
        && grep(search_str, cmd_line[i], value=FALSE))
    {
      return(TRUE)
    }
  }

  return(FALSE)
}
