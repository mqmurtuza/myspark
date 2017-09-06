#### INTIALIZE START#####
print("Running Spark KMART Safety STOCK DC DC DC")
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
  numOfPartitions=350
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
Lead_Time_DC <-19

########################### INTIALIZE END###########################################

#Load Forecast Item final
frcst_item_final_df <- read.df(paste(classpath_data_source,"aat_tbls.frcst_item_final.csv*.gz",sep=""),  source = "com.databricks.spark.csv", delimiter="|")
colnames(frcst_item_final_df) <- c("FISCAL_WK","MODEL_TYPE","SHC_DVSN_CAT_SUBCAT_NO","SHC_ITM","LOCATION_ID","SEAS_CD","SAS_SIZE_PROFILE_ID","SIZE_TYPE_CD","C_THRESHOLD","PVALUE","WEEK1","WEEK2","WEEK3","WEEK4","WEEK5","WEEK6","WEEK7","WEEK8","WEEK9","WEEK10","WEEK11","WEEK12","WEEK13","TOTAL","CALCMAPE","SD95","SD80")
head(frcst_item_final_df)
# createOrReplaceTempView(frcst_item_final_df, "forecast")
# frcst_dv_cat_df<- sql("select *, split(SHC_DVSN_CAT_SUBCAT_NO,'-')[0] as DVSN_NBR,split(SHC_DVSN_CAT_SUBCAT_NO,'-')[1] as CATG_NBR from forecast")
# head(frcst_dv_cat_df)
#count(frcst_dv_cat_df)

#Load Master Location
print("load master location")
extract_mstr_locn_df <- read.df(paste(classpath_data_source,"dataview.locn.txt",sep=""), source = "com.databricks.spark.csv", delimiter="|")
#mstr_locn_df <- as.data.frame(extract_mstr_locn_df[,c(1,3)], drop=false)
locn_df <- select(extract_mstr_locn_df,extract_mstr_locn_df$`_c0`,extract_mstr_locn_df$`_c2`, extract_mstr_locn_df$`_c26`)
colnames(locn_df) <-c("LOCN_NBR","DVSN_NM","CLOSE_DT")
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

# Load LU_SHC_VENDOR_PACK
lu_shc_vendor_pack_df <-read.df(paste(classpath_data_source,"alex_arp_tbls_prd.lu_shc_vendor_pack.*gz",sep=""),  source = "com.databricks.spark.csv", delimiter="|")
lu_shc_vendor_pack_df <- select(lu_shc_vendor_pack_df,lu_shc_vendor_pack_df$`_c11`,lu_shc_vendor_pack_df$`_c30`)
colnames(lu_shc_vendor_pack_df) <- c("ITEM_ID","CORP_3_MO_AVG_COST")
createOrReplaceTempView(lu_shc_vendor_pack_df, "lu_shc_vendor_pack")
lu_shc_vendor_pack_df<- sql("select cast(ITEM_ID as integer), Avg(CORP_3_MO_AVG_COST) as Avg_CORP_3_MO_AVG_COST from lu_shc_vendor_pack GROUP BY ITEM_ID")
showDF(lu_shc_vendor_pack_df)

#Load KSN
ksn_df <- read.df(paste(classpath_data_source,"product.ksn_tbl.*gz",sep=""),  source = "com.databricks.spark.csv", delimiter="|")
ksn_df <-select(ksn_df,ksn_df$`_c0`,ksn_df$`_c2`,ksn_df$`_c5`)
colnames(ksn_df) <- c("KSN_ID","DVSN_NBR","CATG_NBR")
ksn_df <-withColumn(ksn_df,"KSN_ID", cast(ksn_df$KSN_ID,"integer"))
ksn_df <-withColumn(ksn_df,"DVSN_NBR", cast(ksn_df$DVSN_NBR,"integer"))
ksn_df <-withColumn(ksn_df,"CATG_NBR", cast(ksn_df$CATG_NBR,"integer"))
head(ksn_df)

#Join forecast and master location dataframes
print("Join forecast ksn and master location dataframes")
locn_frcst_location_df <- join(kmart_open_locn_df,
                              frcst_item_final_df, 
                              kmart_open_locn_df$LOCN_NBR==frcst_item_final_df$LOCATION_ID)
locn_frcst_location_df <- drop(locn_frcst_location_df,locn_frcst_location_df$LOCN_NBR)
head(locn_frcst_location_df)

#Join forecast-location and ksn dataframes
print("Join forecast and ksn dataframes")
frcst_item_location_ksn_df <- join(locn_frcst_location_df,
                               ksn_df,
                               locn_frcst_location_df$SHC_ITM==ksn_df$KSN_ID
                              )
head(frcst_item_location_ksn_df)

#Join forecast, ksn and lu_shc_vendor_pack_df dataframes
print("Join forecast, ksn and lu_shc_vendor_pack_df dataframes")
frcst_item_location_ksn_vendorpack_df<- join(frcst_item_location_ksn_df,lu_shc_vendor_pack_df,
                                 lu_shc_vendor_pack_df$ITEM_ID==frcst_item_location_ksn_df$KSN_ID
                                 )
head(frcst_item_location_ksn_vendorpack_df)
frcst_item_location_ksn_vendorpack_df<-drop(frcst_item_location_ksn_vendorpack_df,c("ITEM_ID","KSN_ID"))

#Join with Kmart Lead Time
print("Join with Kmart Lead Time")
kmart_raw_data = join(frcst_item_location_ksn_vendorpack_df,
                              kmart_lead_times_df,
                              frcst_item_location_ksn_vendorpack_df$DVSN_NBR == kmart_lead_times_df$Div &
                              frcst_item_location_ksn_vendorpack_df$CATG_NBR == kmart_lead_times_df$Cat
                              )
kmart_raw_data <- drop(kmart_raw_data,c("Div","Cat"))

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

head(kmart_raw_data)
#count(kmart_raw_data)
printSchema(kmart_raw_data)

##############################FUNCTIONS STARTS##############################

calculateEOQ<- function(weekMeasure,Avg_CORP_3_MO_AVG_COST){
   EOQ_DC = sqrt((2*5*weekMeasure*52)/(0.2*(Avg_CORP_3_MO_AVG_COST)))
   return (EOQ_DC)
}

calculateSafetyStock <- function(weekMeasure,CALCMAPE){
  safety_stock_DC <- sqrt(Lead_Time_DC/DAYS_PER_WEEK)*(SAFETY_STOCK_ZFACTOR*((CALCMAPE/100)*weekMeasure))
  return(safety_stock_DC)
}

reorderQty_safetyStock_OptimalInventory <- function(raw_data_df) {
  EOQ_DC_Week1 <- calculateEOQ(raw_data_df$WEEK1, raw_data_df$Avg_CORP_3_MO_AVG_COST)
  EOQ_DC_Week2 <- calculateEOQ(raw_data_df$WEEK2, raw_data_df$Avg_CORP_3_MO_AVG_COST)
  EOQ_DC_Week3 <- calculateEOQ(raw_data_df$WEEK3, raw_data_df$Avg_CORP_3_MO_AVG_COST)
  EOQ_DC_Week4 <- calculateEOQ(raw_data_df$WEEK4, raw_data_df$Avg_CORP_3_MO_AVG_COST)
  EOQ_DC_Week5 <- calculateEOQ(raw_data_df$WEEK5, raw_data_df$Avg_CORP_3_MO_AVG_COST)
  EOQ_DC_Week6 <- calculateEOQ(raw_data_df$WEEK6, raw_data_df$Avg_CORP_3_MO_AVG_COST)
  EOQ_DC_Week7 <- calculateEOQ(raw_data_df$WEEK7, raw_data_df$Avg_CORP_3_MO_AVG_COST)
  EOQ_DC_Week8 <- calculateEOQ(raw_data_df$WEEK8, raw_data_df$Avg_CORP_3_MO_AVG_COST)
  EOQ_DC_Week9 <- calculateEOQ(raw_data_df$WEEK9, raw_data_df$Avg_CORP_3_MO_AVG_COST)
  EOQ_DC_Week10 <-calculateEOQ(raw_data_df$WEEK10, raw_data_df$Avg_CORP_3_MO_AVG_COST)
  EOQ_DC_Week11 <-calculateEOQ(raw_data_df$WEEK11, raw_data_df$Avg_CORP_3_MO_AVG_COST)
  EOQ_DC_Week12 <-calculateEOQ(raw_data_df$WEEK12, raw_data_df$Avg_CORP_3_MO_AVG_COST)
  EOQ_DC_Week13 <-calculateEOQ(raw_data_df$WEEK13, raw_data_df$Avg_CORP_3_MO_AVG_COST)
  
  Safety_Stock_DC_Week1  <- calculateSafetyStock(raw_data_df$WEEK1,raw_data_df$CALCMAPE)
  Safety_Stock_DC_Week2  <- calculateSafetyStock(raw_data_df$WEEK2,raw_data_df$CALCMAPE)
  Safety_Stock_DC_Week3  <- calculateSafetyStock(raw_data_df$WEEK3,raw_data_df$CALCMAPE)
  Safety_Stock_DC_Week4  <- calculateSafetyStock(raw_data_df$WEEK4,raw_data_df$CALCMAPE)
  Safety_Stock_DC_Week5  <- calculateSafetyStock(raw_data_df$WEEK5,raw_data_df$CALCMAPE)
  Safety_Stock_DC_Week6  <- calculateSafetyStock(raw_data_df$WEEK6,raw_data_df$CALCMAPE)
  Safety_Stock_DC_Week7  <- calculateSafetyStock(raw_data_df$WEEK7,raw_data_df$CALCMAPE)
  Safety_Stock_DC_Week8  <- calculateSafetyStock(raw_data_df$WEEK8,raw_data_df$CALCMAPE)
  Safety_Stock_DC_Week9  <- calculateSafetyStock(raw_data_df$WEEK9,raw_data_df$CALCMAPE)
  Safety_Stock_DC_Week10 <- calculateSafetyStock(raw_data_df$WEEK10,raw_data_df$CALCMAPE)
  Safety_Stock_DC_Week11 <- calculateSafetyStock(raw_data_df$WEEK11,raw_data_df$CALCMAPE)
  Safety_Stock_DC_Week12 <- calculateSafetyStock(raw_data_df$WEEK12,raw_data_df$CALCMAPE)
  Safety_Stock_DC_Week13 <- calculateSafetyStock(raw_data_df$WEEK13,raw_data_df$CALCMAPE)

  DC_Optimal_Inventory_Week1  <- EOQ_DC_Week1  + Safety_Stock_DC_Week1
  DC_Optimal_Inventory_Week2  <- EOQ_DC_Week2  + Safety_Stock_DC_Week2
  DC_Optimal_Inventory_Week3  <- EOQ_DC_Week3  + Safety_Stock_DC_Week3
  DC_Optimal_Inventory_Week4  <- EOQ_DC_Week4  + Safety_Stock_DC_Week4
  DC_Optimal_Inventory_Week5  <- EOQ_DC_Week5  + Safety_Stock_DC_Week5
  DC_Optimal_Inventory_Week6  <- EOQ_DC_Week6  + Safety_Stock_DC_Week6
  DC_Optimal_Inventory_Week7  <- EOQ_DC_Week7  + Safety_Stock_DC_Week7
  DC_Optimal_Inventory_Week8  <- EOQ_DC_Week8  + Safety_Stock_DC_Week8
  DC_Optimal_Inventory_Week9  <- EOQ_DC_Week9  + Safety_Stock_DC_Week9
  DC_Optimal_Inventory_Week10 <- EOQ_DC_Week10 + Safety_Stock_DC_Week10
  DC_Optimal_Inventory_Week11 <- EOQ_DC_Week11 + Safety_Stock_DC_Week11
  DC_Optimal_Inventory_Week12 <- EOQ_DC_Week12 + Safety_Stock_DC_Week12
  DC_Optimal_Inventory_Week13 <- EOQ_DC_Week13 + Safety_Stock_DC_Week13
 
  raw_data_df <- cbind(
    raw_data_df,
    EOQ_DC_Week1,
    EOQ_DC_Week2,
    EOQ_DC_Week3,
    EOQ_DC_Week4,
    EOQ_DC_Week5,
    EOQ_DC_Week6,
    EOQ_DC_Week7,
    EOQ_DC_Week8,
    EOQ_DC_Week9,
    EOQ_DC_Week10,
    EOQ_DC_Week11,
    EOQ_DC_Week12,
    EOQ_DC_Week13,
    Safety_Stock_DC_Week1,
    Safety_Stock_DC_Week2,
    Safety_Stock_DC_Week3,
    Safety_Stock_DC_Week4,   
    Safety_Stock_DC_Week5,
    Safety_Stock_DC_Week6, 
    Safety_Stock_DC_Week7, 
    Safety_Stock_DC_Week8, 
    Safety_Stock_DC_Week9, 
    Safety_Stock_DC_Week10,
    Safety_Stock_DC_Week11,
    Safety_Stock_DC_Week12,
    Safety_Stock_DC_Week13,
    DC_Optimal_Inventory_Week1, 
    DC_Optimal_Inventory_Week2,  
    DC_Optimal_Inventory_Week3,   
    DC_Optimal_Inventory_Week4,   
    DC_Optimal_Inventory_Week5,   
    DC_Optimal_Inventory_Week6,   
    DC_Optimal_Inventory_Week7,   
    DC_Optimal_Inventory_Week8,   
    DC_Optimal_Inventory_Week9,   
    DC_Optimal_Inventory_Week10,  
    DC_Optimal_Inventory_Week11,  
    DC_Optimal_Inventory_Week12,
    DC_Optimal_Inventory_Week13
  )
}


##############################FUNCTIONS END##############################

outputSchema <- structType(
  structField("DVSN_NBR", "integer"),
  structField("CATG_NBR", "integer"),
  structField("FISCAL_WK", "string"),
  structField("MODEL_TYPE", "string"),
  structField("SHC_DVSN_CAT_SUBCAT_NO", "string"),
  structField("SHC_ITM", "string"),
  structField("LOCATION_ID", "string"),
  structField("SEAS_CD", "string"),
  structField("SAS_SIZE_PROFILE_ID", "string"),
  structField("SIZE_TYPE_CD", "string"),
  structField("C_THRESHOLD", "double"),
  structField("PVALUE", "double"),
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
  structField("TOTAL", "double"),
  structField("CALCMAPE", "double"),
  structField("SD95", "double"),
  structField("SD80", "string"),
  structField("LdTime", "double"),
  structField("SDLdTime", "double"),
  structField("Avg_CORP_3_MO_AVG_COST", "double"),
  structField("Safety_Stock_DC_Week1", "double"),
  structField("Safety_Stock_DC_Week2", "double"),
  structField("Safety_Stock_DC_Week3", "double"),
  structField("Safety_Stock_DC_Week4", "double"),
  structField("Safety_Stock_DC_Week5", "double"),
  structField("Safety_Stock_DC_Week6", "double"),
  structField("Safety_Stock_DC_Week7", "double"),
  structField("Safety_Stock_DC_Week8", "double"),
  structField("Safety_Stock_DC_Week9", "double"),
  structField("Safety_Stock_DC_Week10", "double"),
  structField("Safety_Stock_DC_Week11", "double"),
  structField("Safety_Stock_DC_Week12", "double"),
  structField("Safety_Stock_DC_Week13", "double"),
  structField("EOQ_DC_Week1", "double"),
  structField("EOQ_DC_Week2", "double"),
  structField("EOQ_DC_Week3", "double"),
  structField("EOQ_DC_Week4", "double"),
  structField("EOQ_DC_Week5", "double"),
  structField("EOQ_DC_Week6", "double"),
  structField("EOQ_DC_Week7", "double"),
  structField("EOQ_DC_Week8", "double"),
  structField("EOQ_DC_Week9", "double"),
  structField("EOQ_DC_Week10", "double"),
  structField("EOQ_DC_Week11", "double"),
  structField("EOQ_DC_Week12", "double"),
  structField("EOQ_DC_Week13", "double"),
  structField("DC_Optimal_Inventory_Week1", "double"),
  structField("DC_Optimal_Inventory_Week2", "double"),
  structField("DC_Optimal_Inventory_Week3", "double"),
  structField("DC_Optimal_Inventory_Week4", "double"),
  structField("DC_Optimal_Inventory_Week5", "double"),
  structField("DC_Optimal_Inventory_Week6", "double"),
  structField("DC_Optimal_Inventory_Week7", "double"),
  structField("DC_Optimal_Inventory_Week8", "double"),
  structField("DC_Optimal_Inventory_Week9", "double"),
  structField("DC_Optimal_Inventory_Week10", "double"),
  structField("DC_Optimal_Inventory_Week11", "double"),
  structField("DC_Optimal_Inventory_Week12", "double"),
  structField("DC_Optimal_Inventory_Week13", "double"),
  structField("CreateDate", "string")
)
print("dapply starts")
safety_df <- repartition(kmart_raw_data, col=kmart_raw_data$LOCATION_ID, numPartitions=numOfPartitions)
final_df <- dapply(safety_df,reorderQty_safetyStock_OptimalInventory,outputSchema)
print("dapply ends")
head(final_df)
write.df(final_df,paste("optimal-inventory-kmart-dc",".csv",sep=""), source="com.databricks.spark.csv", "overwrite")
print("jobs ends")
print("Stopping SparkR Session")
sparkR.stop()
