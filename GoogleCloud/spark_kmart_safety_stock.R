#### INTIALIZE START#####

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
  numOfPartitions=350
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
N_SS_WEEKS <- 13  # Number of future weeks the SS to be calculated

########################### INTIALIZE END###########################################

#Load Forecast Item final
frcst_item_final_df <- read.df(paste(classpath_data_source,"aat_tbls.frcst_item_final.csv*.gz",sep=""),  source = "com.databricks.spark.csv", delimiter="|")
colnames(frcst_item_final_df) <- c("FISCAL_WK","MODEL_TYPE","SHC_DVSN_CAT_SUBCAT_NO","SHC_ITM","LOCATION_ID","SEAS_CD","SAS_SIZE_PROFILE_ID","SIZE_TYPE_CD","C_THRESHOLD","PVALUE","WEEK1","WEEK2","WEEK3","WEEK4","WEEK5","WEEK6","WEEK7","WEEK8","WEEK9","WEEK10","WEEK11","WEEK12","WEEK13","TOTAL","CALCMAPE","SD95","SD80")
head(frcst_item_final_df)
createOrReplaceTempView(frcst_item_final_df, "forecast")
frcst_dv_cat_df<- sql("select *, split(SHC_DVSN_CAT_SUBCAT_NO,'-')[0] as DVSN_NBR,split(SHC_DVSN_CAT_SUBCAT_NO,'-')[1] as CATG_NBR from forecast")
head(frcst_dv_cat_df)
#count(frcst_dv_cat_df)

#Load Master Location
print("load master location")
extract_mstr_locn_df <- read.df(paste(classpath_data_source,"dataview.locn.txt",sep=""), source = "com.databricks.spark.csv", delimiter="|")
#mstr_locn_df <- as.data.frame(extract_mstr_locn_df[,c(1,3)], drop=false)
head(extract_mstr_locn_df)
locn_df <- select(extract_mstr_locn_df,extract_mstr_locn_df$`_c0`,extract_mstr_locn_df$`_c2`, extract_mstr_locn_df$`_c26`)
colnames(locn_df) <-c("LOCN_NBR","DVSN_NM","CLOSE_DT")
head(locn_df)
kmart_locn_df <- filter(locn_df, locn_df$DVSN_NM=="KMART" | locn_df$DVSN_NM=="SUPER K" | locn_df$DVSN_NM=="VARIETY")
kmart_open_locn_df <-filter(kmart_locn_df,kmart_locn_df$CLOSE_DT=="1/1/1900")
head(kmart_open_locn_df)
kmart_open_locn_df <-withColumn(kmart_open_locn_df,"LOCN_NBR",cast(kmart_open_locn_df$LOCN_NBR,"integer"))
#count(kmart_open_locn_df)

# Load Inventory_Opt_Model.Kmart_Lead_Times -(LdTime,SDLdTime)
kmart_lead_times_df <- read.df(paste(classpath_data_source,"inventory_opt_model.kmart_lead_times.*gz",sep=""),  source = "com.databricks.spark.csv", delimiter="|")
colnames(kmart_lead_times_df) <- c("Div","Cat","LdTime","SDLdTime")
createOrReplaceTempView(kmart_lead_times_df, "kmart_lead_times")
kmart_lead_times_df<- sql("select *,cast(current_date() as string) as CreateDate from kmart_lead_times")
head(kmart_lead_times_df)
#count(kmart_lead_times_df)

# #Load KSN
# extract_ksn_df <- read.df(paste(classpath_data_source,"product.ksn_tbl.*gz",sep=""),  source = "com.databricks.spark.csv", delimiter="|")
# ksn_df <-select(extract_ksn_df,extract_ksn_df$`_c0`,extract_ksn_df$`_c2`,extract_ksn_df$`_c5`)
# colnames(ksn_df) <- c("KSN_ID","DVSN_NBR","CATG_NBR")

print("Join forecast and master location dataframes")

#Join forecast and master location dataframes
locn_frcst_df = join(kmart_open_locn_df,
                     frcst_dv_cat_df, 
                     kmart_open_locn_df$LOCN_NBR==frcst_dv_cat_df$LOCATION_ID)
head(locn_frcst_df)
#count(locn_frcst_df)

#Join with Kmart Lead Time

print("Join with Kmart Lead Time")
kmart_raw_data = join(locn_frcst_df,
                              kmart_lead_times_df,
                              locn_frcst_df$DVSN_NBR == kmart_lead_times_df$Div &
                              locn_frcst_df$CATG_NBR == kmart_lead_times_df$Cat
                              )
head(kmart_raw_data)
#count(kmart_raw_data)

print("Casting output schema")
kmart_raw_data <-withColumn(kmart_raw_data,"LdTime", cast(kmart_raw_data$Ldtime,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"SDLdTime", cast(kmart_raw_data$SDLdTime,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"CALCMAPE", cast(kmart_raw_data$CALCMAPE,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"SD95", cast(kmart_raw_data$SD95,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"WEEK1", cast(kmart_raw_data$WEEK1,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"WEEK2", cast(kmart_raw_data$WEEK2,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"WEEK3", cast(kmart_raw_data$WEEK3,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"WEEK4", cast(kmart_raw_data$WEEK4,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"WEEK5", cast(kmart_raw_data$WEEK5,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"WEEK6", cast(kmart_raw_data$WEEK6,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"WEEK7", cast(kmart_raw_data$WEEK7,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"WEEK8", cast(kmart_raw_data$WEEK8,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"WEEK9", cast(kmart_raw_data$WEEK9,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"WEEK10", cast(kmart_raw_data$WEEK10,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"WEEK11", cast(kmart_raw_data$WEEK11,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"WEEK12", cast(kmart_raw_data$WEEK12,"double"))
kmart_raw_data <-withColumn(kmart_raw_data,"WEEK13", cast(kmart_raw_data$WEEK13,"double"))


##############################FUNCTIONS STARTS##############################

calculateReorderQty <- function(weekMeasure,LdTime){
   re_order_qty <-weekMeasure*(LdTime/DAYS_PER_WEEK)
   return (re_order_qty)
}


calculateSafetyStock <- function(weekMeasure,LdTime,CALCMAPE,SDLdTime){
  safety_stock<-sqrt((LdTime/DAYS_PER_WEEK)*((CALCMAPE/(100*SAFETY_STOCK_ZFACTOR))^2)+(SDLdTime/DAYS_PER_WEEK)^2)*(SAFETY_STOCK_ZFACTOR*weekMeasure)
  return(safety_stock)
}

reorderQty_safetyStock_OptimalInventory <- function(raw_data_df) {
  Re_Order_Quantity1 <- calculateReorderQty(raw_data_df$WEEK1, raw_data_df$LdTime)
  Re_Order_Quantity2 <- calculateReorderQty(raw_data_df$WEEK2, raw_data_df$LdTime)
  Re_Order_Quantity3 <- calculateReorderQty(raw_data_df$WEEK3, raw_data_df$LdTime)
  Re_Order_Quantity4 <- calculateReorderQty(raw_data_df$WEEK4, raw_data_df$LdTime)
  Re_Order_Quantity5 <- calculateReorderQty(raw_data_df$WEEK5, raw_data_df$LdTime)
  Re_Order_Quantity6 <- calculateReorderQty(raw_data_df$WEEK6, raw_data_df$LdTime)
  Re_Order_Quantity7 <- calculateReorderQty(raw_data_df$WEEK7, raw_data_df$LdTime)
  Re_Order_Quantity8 <- calculateReorderQty(raw_data_df$WEEK8, raw_data_df$LdTime)
  Re_Order_Quantity9 <- calculateReorderQty(raw_data_df$WEEK9, raw_data_df$LdTime)
  Re_Order_Quantity10 <-calculateReorderQty(raw_data_df$WEEK10, raw_data_df$LdTime)
  Re_Order_Quantity11 <-calculateReorderQty(raw_data_df$WEEK11, raw_data_df$LdTime)
  Re_Order_Quantity12 <-calculateReorderQty(raw_data_df$WEEK12, raw_data_df$LdTime)
  Re_Order_Quantity13 <-calculateReorderQty(raw_data_df$WEEK13, raw_data_df$LdTime)
  
  Safety_Stock_1  <- calculateSafetyStock(raw_data_df$WEEK1,raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)
  Safety_Stock_2  <- calculateSafetyStock(raw_data_df$WEEK2,raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)
  Safety_Stock_3  <- calculateSafetyStock(raw_data_df$WEEK3,raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)
  Safety_Stock_4  <- calculateSafetyStock(raw_data_df$WEEK4,raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)
  Safety_Stock_5  <- calculateSafetyStock(raw_data_df$WEEK5,raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)
  Safety_Stock_6  <- calculateSafetyStock(raw_data_df$WEEK6,raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)
  Safety_Stock_7  <- calculateSafetyStock(raw_data_df$WEEK7,raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)
  Safety_Stock_8  <- calculateSafetyStock(raw_data_df$WEEK8,raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)
  Safety_Stock_9  <- calculateSafetyStock(raw_data_df$WEEK9,raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)
  Safety_Stock_10 <- calculateSafetyStock(raw_data_df$WEEK10,raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)
  Safety_Stock_11 <- calculateSafetyStock(raw_data_df$WEEK11,raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)
  Safety_Stock_12 <- calculateSafetyStock(raw_data_df$WEEK12,raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)
  Safety_Stock_13 <- calculateSafetyStock(raw_data_df$WEEK13,raw_data_df$LdTime,raw_data_df$CALCMAPE,raw_data_df$SDLdTime)

  OptimalInventory_1  <- Re_Order_Quantity1  + Safety_Stock_1
  OptimalInventory_2  <- Re_Order_Quantity2  + Safety_Stock_2
  OptimalInventory_3  <- Re_Order_Quantity3  + Safety_Stock_3
  OptimalInventory_4  <- Re_Order_Quantity4  + Safety_Stock_4
  OptimalInventory_5  <- Re_Order_Quantity5  + Safety_Stock_5
  OptimalInventory_6  <- Re_Order_Quantity6  + Safety_Stock_6
  OptimalInventory_7  <- Re_Order_Quantity7  + Safety_Stock_7
  OptimalInventory_8  <- Re_Order_Quantity8  + Safety_Stock_8
  OptimalInventory_9  <- Re_Order_Quantity9  + Safety_Stock_9
  OptimalInventory_10 <- Re_Order_Quantity10 + Safety_Stock_10
  OptimalInventory_11 <- Re_Order_Quantity11 + Safety_Stock_11
  OptimalInventory_12 <- Re_Order_Quantity12 + Safety_Stock_12
  OptimalInventory_13 <- Re_Order_Quantity13 + Safety_Stock_13

  raw_data_df <- cbind(
    raw_data_df,
    Re_Order_Quantity1,
    Re_Order_Quantity2,
    Re_Order_Quantity3,
    Re_Order_Quantity4,
    Re_Order_Quantity5,
    Re_Order_Quantity6,
    Re_Order_Quantity7,
    Re_Order_Quantity8,
    Re_Order_Quantity9,
    Re_Order_Quantity10,
    Re_Order_Quantity11,
    Re_Order_Quantity12,
    Re_Order_Quantity13,
    Safety_Stock_1,
    Safety_Stock_2,
    Safety_Stock_3,
    Safety_Stock_4,   
    Safety_Stock_5,
    Safety_Stock_6, 
    Safety_Stock_7, 
    Safety_Stock_8, 
    Safety_Stock_9, 
    Safety_Stock_10,
    Safety_Stock_11,
    Safety_Stock_12,
    Safety_Stock_13,
    OptimalInventory_1, 
    OptimalInventory_2,  
    OptimalInventory_3,   
    OptimalInventory_4,   
    OptimalInventory_5,   
    OptimalInventory_6,   
    OptimalInventory_7,   
    OptimalInventory_8,   
    OptimalInventory_9,   
    OptimalInventory_10,  
    OptimalInventory_11,  
    OptimalInventory_12,
    OptimalInventory_13
  )
}


##############################FUNCTIONS END##############################

outputSchema <- structType(
  structField("LOCN_NBR", "integer"),
  structField("DVSN_NM", "string"),
  structField("CLOSE_DT","string"),
  structField("FISCAL_WK", "string"),
  structField("MODEL_TYPE", "string"),
  structField("SHC_DVSN_CAT_SUBCAT_NO", "string"),
  structField("SHC_ITM", "string"),
  structField("LOCATION_ID", "string"),
  structField("SEAS_CD", "string"),
  structField("SAS_SIZE_PROFILE_ID", "string"),
  structField("SIZE_TYPE_CD", "string"),
  structField("C_THRESHOLD", "string"),
  structField("PVALUE", "string"),
  structField("WEEK1", "double"),
  structField("WEEK2", "double"),
  structField("WEEK3", "double"),
  structField("WEEK4", "double"),
  structField("WEEK5", "double"),
  structField("WEEK6", "double"),
  structField("WEEK7", "double"),
  structField("WEEK8", "double"),
  structField("WEEK9", "double"),
  structField("WEEK10", "double"),
  structField("WEEK11", "double"),
  structField("WEEK12", "double"),
  structField("WEEK13", "double"),
  structField("TOTAL", "string"),
  structField("CALCMAPE", "double"),
  structField("SD95", "double"),
  structField("SD80", "string"),
  structField("DVSN_NBR", "string"),
  structField("CATG_NBR", "string"),
  structField("Div", "string"),
  structField("Cat", "string"),
  structField("LdTime", "double"),
  structField("SDLdTime", "double"),
  structField("CreateDate", "string"),
  structField("Re_Order_Quantity1", "double"),
  structField("Re_Order_Quantity2", "double"),
  structField("Re_Order_Quantity3", "double"),
  structField("Re_Order_Quantity4", "double"),
  structField("Re_Order_Quantity5", "double"),
  structField("Re_Order_Quantity6", "double"),
  structField("Re_Order_Quantity7", "double"),
  structField("Re_Order_Quantity8", "double"),
  structField("Re_Order_Quantity9", "double"),
  structField("Re_Order_Quantity10", "double"),
  structField("Re_Order_Quantity11", "double"),
  structField("Re_Order_Quantity12", "double"),
  structField("Re_Order_Quantity13", "double"),
  structField("Safety_Stock1", "double"),
  structField("Safety_Stock2", "double"),
  structField("Safety_Stock3", "double"),
  structField("Safety_Stock4", "double"),
  structField("Safety_Stock5", "double"),
  structField("Safety_Stock6", "double"),
  structField("Safety_Stock7", "double"),
  structField("Safety_Stock8", "double"),
  structField("Safety_Stock9", "double"),
  structField("Safety_Stock10", "double"),
  structField("Safety_Stock11", "double"),
  structField("Safety_Stock12", "double"),
  structField("Safety_Stock13", "double"),
  structField("OptimalInventory1", "double"),
  structField("OptimalInventory2", "double"),
  structField("OptimalInventory3", "double"),
  structField("OptimalInventory4", "double"),
  structField("OptimalInventory5", "double"),
  structField("OptimalInventory6", "double"),
  structField("OptimalInventory7", "double"),
  structField("OptimalInventory8", "double"),
  structField("OptimalInventory9", "double"),
  structField("OptimalInventory10", "double"),
  structField("OptimalInventory11", "double"),
  structField("OptimalInventory12", "double"),
  structField("OptimalInventory13", "double")
)
print("dapply starts")
new_df <- repartition(kmart_raw_data, col=kmart_raw_data$LOCN_NBR, numPartitions=numOfPartitions)

final_df <- dapply(new_df,reorderQty_safetyStock_OptimalInventory,outputSchema)
print("dapply ends")
head(final_df)
final_df<- drop(final_df, final_df$LOCN_NBR)
write.df(final_df,paste("optimal-inventory-kmart",".csv",sep=""), source="com.databricks.spark.csv", "overwrite")
print("jobs ends")
print("Stopping SparkR Session")
sparkR.stop()
