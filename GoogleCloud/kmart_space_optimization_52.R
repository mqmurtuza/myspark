#### INTIALIZE START#####
print("Running Spark KMART Safety OPTIMIZATION")
print("Running Spark KMART Safety OPTIMIZATION")
print("Running Spark KMART Safety OPTIMIZATION")
print("Running Spark KMART Safety OPTIMIZATION")
args = commandArgs(trailingOnly=TRUE)
if(is.na(args[1])){
  targetEnv="local"
}else{
  targetEnv<-args[1]
}
if(!is.na(args[2])){
  classpath_datefolder =args[2]
}
print(targetEnv)
if(targetEnv=="google" | targetEnv=="amazon"){
  classpath_data_source = paste("optimal_inventory/",classpath_datefolder,"/",sep="")
  if(targetEnv=="google"){
    classpath_data_source=paste("gs://shc-afe-dev.appspot.com/",classpath_data_source,sep="")
  }else{
    classpath_data_source=paste("s3://shc-afe-dev/",classpath_data_source,sep="")
  }
  numOfPartitions=800
}else{
  classpath_data_source = "/home/osboxes/sears/fi-safety-stock/dataseed/kmart/"
  numOfPartitions=50
}
print(classpath_data_source)
print(numOfPartitions)
spark_path <- "/home/osboxes/spark/spark-2.0.2-bin-hadoop2.7"
#spark_path = "/usr/lib/spark"

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = spark_path)
}
print(Sys.getenv("SPARK_HOME"))
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sc <- sparkR.session(appName = "safetystock_inventory")

# Parameters for Safety Stock module
SAFETY_STOCK_ZFACTOR <- 1.96
DAYS_PER_WEEK <- 7
N_SS_WEEKS <- 52  # Number of future weeks the SS to be calculated
k <-1+(3.322*log10(N_SS_WEEKS))

########################### INTIALIZE END###########################################

#Load Forecast Item final
frcst_item_final_df <- read.df(paste(classpath_data_source,"item_frcst.csv",sep=""), source = "com.databricks.spark.csv", delimiter=",")
colnames(frcst_item_final_df) <-c("FISCAL_WK", "DIV_NBR", "CAT_NBR", "SUBCAT","SEAS_CD", "FORECAST_GRP","SHC_ITM","LOCATION_ID","FORMAT","PVALUE", "MODEL", "CALCMAPE","SD95","SD80","WEEK1", "WEEK2", "WEEK3", "WEEK4", "WEEK5","WEEK6","WEEK7","WEEK8", "WEEK9", "WEEK10","WEEK11","WEEK12","WEEK13","WEEK14","WEEK15","WEEK16","WEEK17","WEEK18","WEEK19","WEEK20","WEEK21","WEEK22","WEEK23","WEEK24","WEEK25","WEEK26","WEEK27","WEEK28","WEEK29","WEEK30","WEEK31","WEEK32","WEEK33","WEEK34","WEEK35","WEEK36","WEEK37","WEEK38","WEEK39","WEEK40","WEEK41","WEEK42","WEEK43","WEEK44","WEEK45","WEEK46","WEEK47","WEEK48","WEEK49","WEEK50","WEEK51","WEEK52")
frcst_item_final_df<- withColumn(frcst_item_final_df,"FISCAL_WK", cast(frcst_item_final_df$FISCAL_WK,"integer"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"DIV_NBR", cast(frcst_item_final_df$DIV_NBR,"integer"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"CAT_NBR", cast(frcst_item_final_df$CAT_NBR,"integer"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"SHC_ITM", cast(frcst_item_final_df$SHC_ITM,"integer"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"LOCATION_ID", cast(frcst_item_final_df$LOCATION_ID,"integer"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK1", cast(frcst_item_final_df$WEEK1,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK1", cast(frcst_item_final_df$WEEK1,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK2", cast(frcst_item_final_df$WEEK2,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK3", cast(frcst_item_final_df$WEEK3,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK4", cast(frcst_item_final_df$WEEK4,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK5", cast(frcst_item_final_df$WEEK5,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK6", cast(frcst_item_final_df$WEEK6,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK7", cast(frcst_item_final_df$WEEK7,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK8", cast(frcst_item_final_df$WEEK8,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK9", cast(frcst_item_final_df$WEEK9,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK10", cast(frcst_item_final_df$WEEK10,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK11", cast(frcst_item_final_df$WEEK11,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK12", cast(frcst_item_final_df$WEEK12,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK13", cast(frcst_item_final_df$WEEK13,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK14", cast(frcst_item_final_df$WEEK14,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK15", cast(frcst_item_final_df$WEEK15,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK16", cast(frcst_item_final_df$WEEK16,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK17", cast(frcst_item_final_df$WEEK17,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK18", cast(frcst_item_final_df$WEEK18,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK19", cast(frcst_item_final_df$WEEK19,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK20", cast(frcst_item_final_df$WEEK20,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK21", cast(frcst_item_final_df$WEEK21,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK22", cast(frcst_item_final_df$WEEK22,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK23", cast(frcst_item_final_df$WEEK23,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK24", cast(frcst_item_final_df$WEEK24,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK25", cast(frcst_item_final_df$WEEK25,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK26", cast(frcst_item_final_df$WEEK26,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK27", cast(frcst_item_final_df$WEEK27,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK28", cast(frcst_item_final_df$WEEK28,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK29", cast(frcst_item_final_df$WEEK29,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK30", cast(frcst_item_final_df$WEEK30,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK31", cast(frcst_item_final_df$WEEK31,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK32", cast(frcst_item_final_df$WEEK32,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK33", cast(frcst_item_final_df$WEEK33,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK34", cast(frcst_item_final_df$WEEK34,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK35", cast(frcst_item_final_df$WEEK35,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK36", cast(frcst_item_final_df$WEEK36,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK37", cast(frcst_item_final_df$WEEK37,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK38", cast(frcst_item_final_df$WEEK38,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK39", cast(frcst_item_final_df$WEEK39,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK40", cast(frcst_item_final_df$WEEK40,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK41", cast(frcst_item_final_df$WEEK41,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK42", cast(frcst_item_final_df$WEEK42,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK43", cast(frcst_item_final_df$WEEK43,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK44", cast(frcst_item_final_df$WEEK44,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK45", cast(frcst_item_final_df$WEEK45,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK46", cast(frcst_item_final_df$WEEK46,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK47", cast(frcst_item_final_df$WEEK47,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK48", cast(frcst_item_final_df$WEEK48,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK49", cast(frcst_item_final_df$WEEK49,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK50", cast(frcst_item_final_df$WEEK50,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK51", cast(frcst_item_final_df$WEEK51,"double"))
frcst_item_final_df<- withColumn(frcst_item_final_df,"WEEK52", cast(frcst_item_final_df$WEEK52,"double"))

#frcst_item_final_df <- filter(frcst_item_final_df,frcst_item_final_df$LOCATION_ID==4292)
#head(frcst_item_final_df)

# Load Inventory_Opt_Model.Kmart_Lead_Times -(LdTime,SDLdTime)
kmart_lead_times_df <- read.df(paste(classpath_data_source,"inventory_opt_model.kmart_lead_times.*gz",sep=""),  source = "com.databricks.spark.csv", delimiter="|")
colnames(kmart_lead_times_df) <- c("Div","Cat","LdTime","SDLdTime")
createOrReplaceTempView(kmart_lead_times_df, "kmart_lead_times")
kmart_lead_times_df<- sql("SELECT 
                          CAST(Div as integer) as Div,
                          CAST(Cat as integer) as Cat,
                          LdTime,
                          SDLdTime,
                          CAST(current_date() as string) as CreateDate
                          from kmart_lead_times")
#head(kmart_lead_times_df)
#count(kmart_lead_times_df)

######JOINING STARTS********************************************************************

#Join with Kmart Lead Time
print("Join with Kmart Lead Time")
kmart_raw_data = join(frcst_item_final_df,
                              kmart_lead_times_df,
                              frcst_item_final_df$DIV_NBR == kmart_lead_times_df$Div &
                              frcst_item_final_df$CAT_NBR == kmart_lead_times_df$Cat
                              )
kmart_raw_data <- drop(kmart_raw_data,c("Div","Cat"))
#head(kmart_raw_data)
#count(kmart_raw_data)
#****************************Joining Ends***************************#
print("Casting output schema")
kmart_raw_data <-withColumn(kmart_raw_data,"LdTime", cast(kmart_raw_data$Ldtime,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"SDLdTime", cast(kmart_raw_data$SDLdTime,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"CALCMAPE", cast(kmart_raw_data$CALCMAPE,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"SD95", cast(kmart_raw_data$SD95,"double"))
printSchema(kmart_raw_data)

##############################FUNCTIONS STARTS##############################

calculateReorderQty <- function(weekMeasure,LdTime){
   re_order_qty <-weekMeasure*(LdTime/DAYS_PER_WEEK)
   return (re_order_qty)
}

reOrderQtyDataFrame <- function(raw_data_df){
  for(wk in 1:N_SS_WEEKS){
    weekLbl <- paste0("WEEK",wk)
    ReOrdQtyLbl <- paste0("ReOrdQty",wk)
    raw_data_df[ReOrdQtyLbl]<-calculateReorderQty(raw_data_df[weekLbl], raw_data_df$LdTime)
  }
  return(raw_data_df)
}

calculateSafetyStock <- function(weekMeasure,LdTime,CALCMAPE,SDLdTime){
  safety_stock<-sqrt((LdTime/DAYS_PER_WEEK)*((CALCMAPE/(100*SAFETY_STOCK_ZFACTOR))^2)+(SDLdTime/DAYS_PER_WEEK)^2)*(SAFETY_STOCK_ZFACTOR*weekMeasure)
  return(safety_stock)
}

safetyStockDataFrame <- function(raw_data_df){
  for(wk in 1:N_SS_WEEKS){
    weekLbl <- paste0("WEEK",wk)
    safetyStockLbl <- paste0("SS",wk)
    raw_data_df[safetyStockLbl]<-calculateSafetyStock(raw_data_df[weekLbl],raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)
  }
  return(raw_data_df)
}

optimalInventoryDataFrame <- function(raw_data_df){
  for(wk in 1:N_SS_WEEKS){
    weekLbl <- paste0("WEEK",wk)
    ReOrdQtyLbl <- paste0("ReOrdQty",wk)
    safetyStockLbl <- paste0("SS",wk)
    optimalInvLbl <- paste0("OI",wk)
    raw_data_df[optimalInvLbl]<-raw_data_df[ReOrdQtyLbl] + raw_data_df[safetyStockLbl]
  }
  return(raw_data_df)
}

reorderQty_safetyStock_OptimalInventory <- function(raw_data_df) {
  reorderdf <- reOrderQtyDataFrame(raw_data_df)
  re_ss_df  <- safetyStockDataFrame(reorderdf)
  re_ss_oo_df <-optimalInventoryDataFrame(re_ss_df)
  max_min_ran_occ_hc_df<- calculateHighestSpaceCapacity(re_ss_oo_df)
  return(max_min_ran_occ_hc_df)
}

calculateHighestSpaceCapacity <- function(re_ss_oo_df){
  
  #converting into R dataframes as some of R functions doesn't work with sparkDataframe
  re_ss_oo_df <- as.data.frame(re_ss_oo_df)
  re_ss_oo_df$PcMax <- 0
  re_ss_oo_df$PcMin <- 0
  
  re_ss_oo_df$RI <- 0
  re_ss_oo_df$RM1 <-0
  re_ss_oo_df$RM2 <-0
  re_ss_oo_df$RM3 <-0
  re_ss_oo_df$RM4 <-0
  re_ss_oo_df$RM5 <-0
  re_ss_oo_df$RM6 <-0
  re_ss_oo_df$RM7 <-0

  re_ss_oo_df$OR1<-0
  re_ss_oo_df$OR2<-0
  re_ss_oo_df$OR3<-0
  re_ss_oo_df$OR4<-0
  re_ss_oo_df$OR5<-0
  re_ss_oo_df$OR6<-0
  re_ss_oo_df$OR7<-0
  re_ss_oo_df$HighestSpaceCapacity<-0

  for(i in 1:nrow(re_ss_oo_df)){

    listOfOptimalInventoryWeeks <- c()
    for (wk in 1:52) {
      OptimalInventory_Lbl <- paste0("OI", wk)
      listOfOptimalInventoryWeeks <-c(listOfOptimalInventoryWeeks,re_ss_oo_df[i,OptimalInventory_Lbl])
    }

    re_ss_oo_df[i,"PcMax"] <-max(listOfOptimalInventoryWeeks)
    re_ss_oo_df[i,"PcMin"] <-min(listOfOptimalInventoryWeeks)

    re_ss_oo_df[i,"RI"] <- (re_ss_oo_df[i,"PcMax"]-re_ss_oo_df[i,"PcMin"])/k
    re_ss_oo_df[i,"RM1"] <- re_ss_oo_df[i,"PcMin"] + (re_ss_oo_df[i,"RI"]-1)
    re_ss_oo_df[i,"RM2"] <- re_ss_oo_df[i,"RM1"]+ re_ss_oo_df[i,"RI"]
    re_ss_oo_df[i,"RM3"] <- re_ss_oo_df[i,"RM2"]+ re_ss_oo_df[i,"RI"]
    re_ss_oo_df[i,"RM4"] <- re_ss_oo_df[i,"RM3"]+ re_ss_oo_df[i,"RI"]
    re_ss_oo_df[i,"RM5"] <- re_ss_oo_df[i,"RM4"]+ re_ss_oo_df[i,"RI"]
    re_ss_oo_df[i,"RM6"] <- re_ss_oo_df[i,"RM5"]+ re_ss_oo_df[i,"RI"]
    re_ss_oo_df[i,"RM7"] <- re_ss_oo_df[i,"PcMax"]

    for (wk in 1:52) {
      OptimalInventory_Lbl <- paste0("OI", wk)
      if (re_ss_oo_df[i,OptimalInventory_Lbl] >= re_ss_oo_df[i,"PcMin"] && re_ss_oo_df[i,OptimalInventory_Lbl] <= re_ss_oo_df[i,"RM1"]) {
        re_ss_oo_df[i,"OR1"] <- re_ss_oo_df[i,"OR1"] + 1

      } else
        if (re_ss_oo_df[i,OptimalInventory_Lbl] > re_ss_oo_df[i,"RM1"] && re_ss_oo_df[i,OptimalInventory_Lbl] <= re_ss_oo_df[i,"RM2"]) {
          re_ss_oo_df[i,"OR2"] <- re_ss_oo_df[i,"OR2"] + 1

        }  else
          if (re_ss_oo_df[i,OptimalInventory_Lbl] > re_ss_oo_df[i,"RM2"] && re_ss_oo_df[i,OptimalInventory_Lbl] <= re_ss_oo_df[i,"RM3"]) {
            re_ss_oo_df[i,"OR3"] <- re_ss_oo_df[i,"OR3"] + 1

          }  else
            if (re_ss_oo_df[i,OptimalInventory_Lbl] > re_ss_oo_df[i,"RM3"] && re_ss_oo_df[i,OptimalInventory_Lbl] <= re_ss_oo_df[i,"RM4"]) {
              re_ss_oo_df[i,"OR4"] <- re_ss_oo_df[i,"OR4"] + 1
            }  else
              if (re_ss_oo_df[i,OptimalInventory_Lbl] > re_ss_oo_df[i,"RM4"] && re_ss_oo_df[i,OptimalInventory_Lbl] <= re_ss_oo_df[i,"RM5"]) {
                re_ss_oo_df[i,"OR5"] <- re_ss_oo_df[i,"OR5"] + 1
              }
                else
                  if (re_ss_oo_df[i,OptimalInventory_Lbl] > re_ss_oo_df[i,"RM5"] && re_ss_oo_df[i,OptimalInventory_Lbl] <= re_ss_oo_df[i,"RM6"]) {
                    re_ss_oo_df[i,"OR6"] <- re_ss_oo_df[i,"OR6"] + 1
                  }
                  else
                    if (re_ss_oo_df[i,OptimalInventory_Lbl] > re_ss_oo_df[i,"RM6"] && re_ss_oo_df[i,OptimalInventory_Lbl] <= re_ss_oo_df[i,"RM7"]) {
                      re_ss_oo_df[i,"OR7"] <- re_ss_oo_df[i,"OR7"] + 1
                    }
    }

    #Determine the Highest Capacity
    maxOcValue<-0
    for(wkNbr in 1:7){
      oclbl <- paste0("OR",wkNbr)
      rmlbl <- paste0("RM",wkNbr)
      if (maxOcValue <= re_ss_oo_df[i,oclbl]) {
        maxOcValue <- re_ss_oo_df[i,oclbl]
        re_ss_oo_df[i,"HighestSpaceCapacity"] <- re_ss_oo_df[i,rmlbl]
      }
    }
  }
  return(re_ss_oo_df)
}


##############################FUNCTIONS END##############################

##############################OUTPUT SCHEMA#################################
outputSchema <- structType(
  structField("FISCAL_WK","integer"),
  structField("DIV_NBR","integer"),
  structField("CAT_NBR","integer"),
  structField("SUBCAT","string"),
  structField("SEAS_CD","string"),
  structField("FORECAST_GRP","string"),
  structField("SHC_ITM","integer"),
  structField("LOCATION_ID","integer"),
  structField("FORMAT","string"),
  structField("PVALUE","string"),
  structField("MODEL","string"),
  structField("CALCMAPE","double"),
  structField("SD95","double"),
  structField("SD80","string"),
  structField("WEEK1","double"),
  structField("WEEK2","double"),
  structField("WEEK3","double"),
  structField("WEEK4","double"),
  structField("WEEK5","double"),
  structField("WEEK6","double"),
  structField("WEEK7","double"),
  structField("WEEK8","double"),
  structField("WEEK9","double"),
  structField("WEEK10","double"),
  structField("WEEK11","double"),
  structField("WEEK12","double"),
  structField("WEEK13","double"),
  structField("WEEK14","double"),
  structField("WEEK15","double"),
  structField("WEEK16","double"),
  structField("WEEK17","double"),
  structField("WEEK18","double"),
  structField("WEEK19","double"),
  structField("WEEK20","double"),
  structField("WEEK21","double"),
  structField("WEEK22","double"),
  structField("WEEK23","double"),
  structField("WEEK24","double"),
  structField("WEEK25","double"),
  structField("WEEK26","double"),
  structField("WEEK27","double"),
  structField("WEEK28","double"),
  structField("WEEK29","double"),
  structField("WEEK30","double"),
  structField("WEEK31","double"),
  structField("WEEK32","double"),
  structField("WEEK33","double"),
  structField("WEEK34","double"),
  structField("WEEK35","double"),
  structField("WEEK36","double"),
  structField("WEEK37","double"),
  structField("WEEK38","double"),
  structField("WEEK39","double"),
  structField("WEEK40","double"),
  structField("WEEK41","double"),
  structField("WEEK42","double"),
  structField("WEEK43","double"),
  structField("WEEK44","double"),
  structField("WEEK45","double"),
  structField("WEEK46","double"),
  structField("WEEK47","double"),
  structField("WEEK48","double"),
  structField("WEEK49","double"),
  structField("WEEK50","double"),
  structField("WEEK51","double"),
  structField("WEEK52","double"),
  structField("LdTime","double"),
  structField("SDLdTime","double"),
  structField("CreateDate","string"),
  structField("ReOrdQty1","double"),
  structField("ReOrdQty2","double"),
  structField("ReOrdQty3","double"),
  structField("ReOrdQty4","double"),
  structField("ReOrdQty5","double"),
  structField("ReOrdQty6","double"),
  structField("ReOrdQty7","double"),
  structField("ReOrdQty8","double"),
  structField("ReOrdQty9","double"),
  structField("ReOrdQty10","double"),
  structField("ReOrdQty11","double"),
  structField("ReOrdQty12","double"),
  structField("ReOrdQty13","double"),
  structField("ReOrdQty14","double"),
  structField("ReOrdQty15","double"),
  structField("ReOrdQty16","double"),
  structField("ReOrdQty17","double"),
  structField("ReOrdQty18","double"),
  structField("ReOrdQty19","double"),
  structField("ReOrdQty20","double"),
  structField("ReOrdQty21","double"),
  structField("ReOrdQty22","double"),
  structField("ReOrdQty23","double"),
  structField("ReOrdQty24","double"),
  structField("ReOrdQty25","double"),
  structField("ReOrdQty26","double"),
  structField("ReOrdQty27","double"),
  structField("ReOrdQty28","double"),
  structField("ReOrdQty29","double"),
  structField("ReOrdQty30","double"),
  structField("ReOrdQty31","double"),
  structField("ReOrdQty32","double"),
  structField("ReOrdQty33","double"),
  structField("ReOrdQty34","double"),
  structField("ReOrdQty35","double"),
  structField("ReOrdQty36","double"),
  structField("ReOrdQty37","double"),
  structField("ReOrdQty38","double"),
  structField("ReOrdQty39","double"),
  structField("ReOrdQty40","double"),
  structField("ReOrdQty41","double"),
  structField("ReOrdQty42","double"),
  structField("ReOrdQty43","double"),
  structField("ReOrdQty44","double"),
  structField("ReOrdQty45","double"),
  structField("ReOrdQty46","double"),
  structField("ReOrdQty47","double"),
  structField("ReOrdQty48","double"),
  structField("ReOrdQty49","double"),
  structField("ReOrdQty50","double"),
  structField("ReOrdQty51","double"),
  structField("ReOrdQty52","double"),
  structField("SS1","double"),
  structField("SS2","double"),
  structField("SS3","double"),
  structField("SS4","double"),
  structField("SS5","double"),
  structField("SS6","double"),
  structField("SS7","double"),
  structField("SS8","double"),
  structField("SS9","double"),
  structField("SS10","double"),
  structField("SS11","double"),
  structField("SS12","double"),
  structField("SS13","double"),
  structField("SS14","double"),
  structField("SS15","double"),
  structField("SS16","double"),
  structField("SS17","double"),
  structField("SS18","double"),
  structField("SS19","double"),
  structField("SS20","double"),
  structField("SS21","double"),
  structField("SS22","double"),
  structField("SS23","double"),
  structField("SS24","double"),
  structField("SS25","double"),
  structField("SS26","double"),
  structField("SS27","double"),
  structField("SS28","double"),
  structField("SS29","double"),
  structField("SS30","double"),
  structField("SS31","double"),
  structField("SS32","double"),
  structField("SS33","double"),
  structField("SS34","double"),
  structField("SS35","double"),
  structField("SS36","double"),
  structField("SS37","double"),
  structField("SS38","double"),
  structField("SS39","double"),
  structField("SS40","double"),
  structField("SS41","double"),
  structField("SS42","double"),
  structField("SS43","double"),
  structField("SS44","double"),
  structField("SS45","double"),
  structField("SS46","double"),
  structField("SS47","double"),
  structField("SS48","double"),
  structField("SS49","double"),
  structField("SS50","double"),
  structField("SS51","double"),
  structField("SS52","double"),
  structField("OI1","double"),
  structField("OI2","double"),
  structField("OI3","double"),
  structField("OI4","double"),
  structField("OI5","double"),
  structField("OI6","double"),
  structField("OI7","double"),
  structField("OI8","double"),
  structField("OI9","double"),
  structField("OI10","double"),
  structField("OI11","double"),
  structField("OI12","double"),
  structField("OI13","double"),
  structField("OI14","double"),
  structField("OI15","double"),
  structField("OI16","double"),
  structField("OI17","double"),
  structField("OI18","double"),
  structField("OI19","double"),
  structField("OI20","double"),
  structField("OI21","double"),
  structField("OI22","double"),
  structField("OI23","double"),
  structField("OI24","double"),
  structField("OI25","double"),
  structField("OI26","double"),
  structField("OI27","double"),
  structField("OI28","double"),
  structField("OI29","double"),
  structField("OI30","double"),
  structField("OI31","double"),
  structField("OI32","double"),
  structField("OI33","double"),
  structField("OI34","double"),
  structField("OI35","double"),
  structField("OI36","double"),
  structField("OI37","double"),
  structField("OI38","double"),
  structField("OI39","double"),
  structField("OI40","double"),
  structField("OI41","double"),
  structField("OI42","double"),
  structField("OI43","double"),
  structField("OI44","double"),
  structField("OI45","double"),
  structField("OI46","double"),
  structField("OI47","double"),
  structField("OI48","double"),
  structField("OI49","double"),
  structField("OI50","double"),
  structField("OI51","double"),
  structField("OI52","double"),
  structField("PcMax", "double"),
  structField("PcMin", "double"),
  structField("RI", "double"),
  structField("RM1", "double"),
  structField("RM2", "double"),
  structField("RM3", "double"),
  structField("RM4", "double"),
  structField("RM5", "double"),
  structField("RM6", "double"),
  structField("RM7", "double"),
  structField("OR1", "double"),
  structField("OR2", "double"),
  structField("OR3", "double"),
  structField("OR4", "double"),
  structField("OR5", "double"),
  structField("OR6", "double"),
  structField("OR7", "double"),
  structField("HighestSpaceCapacity", "double")
)

#################OUTPUT SCHEMA ENDS#########################################################

print("dapply starts")
#partitionCount <- count(distinct(select(kmart_raw_data,"LOCATION_ID")))
new_df <- repartition(kmart_raw_data, col=kmart_raw_data$LOCATION_ID,kmart_raw_data$DIV_NBR, numPartitions=numOfPartitions)
persist(kmart_raw_data,"MEMORY_AND_DISK")
final_df <- dapply(new_df,reorderQty_safetyStock_OptimalInventory,outputSchema)
head(final_df)
print("dapply ends")
write.df(final_df,"optimal-space-optimization-kmart.csv", source="com.databricks.spark.csv", "overwrite")
print("jobs ends")
print("Stopping SparkR Session")
sparkR.stop()
