#### INTIALIZE START#####
print("Running Spark SEARS Safety STOCK DC DC DC")
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
  numOfPartitions=500
}else{
  classpath_data_source = "/home/osboxes/sears/fi-safety-stock/dataseed/sears/"
  numOfPartitions=300
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
print("Load Forecast Item final")
frcst_item_final_df <- read.df(paste(classpath_data_source,"aat_tbls.frcst_item_final.csv*.gz",sep=""),  source = "com.databricks.spark.csv", delimiter="|")
colnames(frcst_item_final_df) <- c("FISCAL_WK","MODEL_TYPE","SHC_DVSN_CAT_SUBCAT_NO","SHC_ITM","LOCATION_ID","SEAS_CD","SAS_SIZE_PROFILE_ID","SIZE_TYPE_CD","C_THRESHOLD","PVALUE","WEEK1","WEEK2","WEEK3","WEEK4","WEEK5","WEEK6","WEEK7","WEEK8","WEEK9","WEEK10","WEEK11","WEEK12","WEEK13","TOTAL","CALCMAPE","SD95","SD80")
head(frcst_item_final_df)
#count(frcst_item_final_df)

#Load Master Location
print("load master location")
extract_mstr_locn_df <- read.df(paste(classpath_data_source,"dataview.locn.txt",sep=""), source = "com.databricks.spark.csv", delimiter="|")
locn_df <- select(extract_mstr_locn_df,extract_mstr_locn_df$`_c0`,extract_mstr_locn_df$`_c2`, extract_mstr_locn_df$`_c26`)
colnames(locn_df) <-c("LOCN_NBR","DVSN_NM","CLOSE_DT")
sears_open_locn_df <- filter(locn_df, locn_df$DVSN_NM=="SEARS" & locn_df$CLOSE_DT=="1/1/1900")
head(sears_open_locn_df)
sears_open_locn_df <-withColumn(sears_open_locn_df,"LOCN_NBR",cast(sears_open_locn_df$LOCN_NBR,"integer"))
#count(sears_open_locn_df)

# Load Inventory_Opt_Model.sears_Lead_Times -(LdTime,SDLdTime)
print("Loading Inventory_Opt_Model.sears_Lead_Times")
sears_lead_times_df <- read.df(paste(classpath_data_source,"inventory_opt_model.sears_lead_times.*gz",sep=""),  source = "com.databricks.spark.csv", delimiter="|")
colnames(sears_lead_times_df) <- c("Div","Line","LdTime","SDLdTime")
createOrReplaceTempView(sears_lead_times_df, "sears_lead_times")
sears_lead_times_df<- sql("select *,cast(current_date() as string) as CreateDate from sears_lead_times")
head(sears_lead_times_df)
#count(sears_lead_times_df)

#Loading Product Table
print("Loading Sears Product Table")
sprs_product_df <- read.df(paste(classpath_data_source,"sprs_tbls.product.csv*.gz",sep=""), source = "com.databricks.spark.csv", delimiter="|")
sprs_product_df <- select(sprs_product_df,sprs_product_df$`_c0`,sprs_product_df$`_c1`, sprs_product_df$`_c4`,sprs_product_df$`_c6`,sprs_product_df$`_c7`)
colnames(sprs_product_df) <-c("PRD_IRL_NO","ITM_NO","ITM_PRG_DT","DIV_NO","LN_NO")
sprs_product_df <-withColumn(sprs_product_df,"ITM_NO",cast(sprs_product_df$ITM_NO,"integer"))
sprs_product_df <-withColumn(sprs_product_df,"DIV_NO",cast(sprs_product_df$DIV_NO,"integer"))
sprs_product_df <- filter(sprs_product_df,isNull(sprs_product_df$ITM_PRG_DT))
print("after sprs product filter")
#count(sprs_product_df)
head(sprs_product_df)

#"Loading Sears Product SKU Table
print("Loading Sears Product SKU Table")
sprs_product_sku_df <- read.df(paste(classpath_data_source,"sprs_tbls.product_sku.csv*.gz",sep=""), source = "com.databricks.spark.csv", delimiter="|")
sprs_product_sku_df <- select(sprs_product_sku_df,sprs_product_sku_df$`_c0`,sprs_product_sku_df$`_c1`, sprs_product_sku_df$`_c2`)
colnames(sprs_product_sku_df) <-c("SKU_PRD_IRL_NO","SKU_NO","SKU_PRG_DT")
sprs_product_sku_df <-withColumn(sprs_product_sku_df,"SKU_NO",cast(sprs_product_sku_df$SKU_NO,"integer"))
sprs_product_sku_df <- filter(sprs_product_sku_df,isNull(sprs_product_sku_df$SKU_PRG_DT))
print("after sprs product sku filter")
#count(sprs_product_sku_df)
head(sprs_product_sku_df)

#Loading Sears CORE_BRIDGE_SKU Table
print("Loading Sears product CORE_BRIDGE_SKU Table")
sprs_product_core_bridge_sku_df <- read.df(paste(classpath_data_source,"product.core_bridge_sku_tbl.csv*.gz",sep=""), source = "com.databricks.spark.csv", delimiter="|")
sprs_product_core_bridge_sku_df <- select(sprs_product_core_bridge_sku_df,sprs_product_core_bridge_sku_df$`_c0`,sprs_product_core_bridge_sku_df$`_c1`, sprs_product_core_bridge_sku_df$`_c2`,sprs_product_core_bridge_sku_df$`_c3`)
colnames(sprs_product_core_bridge_sku_df) <-c("KSN_ID","SRS_DIV_NO","SRS_ITM_NO","SRS_SKU_NO")
sprs_product_core_bridge_sku_df <-withColumn(sprs_product_core_bridge_sku_df,"KSN_ID",cast(sprs_product_core_bridge_sku_df$KSN_ID,"integer"))
sprs_product_core_bridge_sku_df <-withColumn(sprs_product_core_bridge_sku_df,"SRS_DIV_NO",cast(sprs_product_core_bridge_sku_df$SRS_DIV_NO,"integer"))
sprs_product_core_bridge_sku_df <-withColumn(sprs_product_core_bridge_sku_df,"SRS_ITM_NO",cast(sprs_product_core_bridge_sku_df$SRS_ITM_NO,"integer"))
sprs_product_core_bridge_sku_df <-withColumn(sprs_product_core_bridge_sku_df,"SRS_SKU_NO",cast(sprs_product_core_bridge_sku_df$SRS_SKU_NO,"integer"))
#count(sprs_product_core_bridge_sku_df)
head(sprs_product_core_bridge_sku_df)

#Loading ALEX_ARP_VIEWS_PRD.LU_SRS_PRODUCT
print("Loading ALEX_ARP_VIEWS_PRD.LU_SRS_PRODUCT")
arp_lu_srs_product_df <- read.df(paste(classpath_data_source,"alex_arp_tbls_prd.lu_srs_product*.gz",sep=""), source = "com.databricks.spark.csv", delimiter="|")
arp_lu_srs_product_df<- select(arp_lu_srs_product_df,arp_lu_srs_product_df$`_c5`,arp_lu_srs_product_df$`_c13`,arp_lu_srs_product_df$`_c19`)
colnames(arp_lu_srs_product_df) <- c("DIV_NBR","ITM_NBR","NATL_CST_PRC")
createOrReplaceTempView(arp_lu_srs_product_df,"lu_srs_product")
arp_lu_srs_product_df <- sql("select cast(DIV_NBR as integer),cast(ITM_NBR as integer),Avg(NATL_CST_PRC) as NATL_CST_PRC FROM lu_srs_product GROUP BY 1,2")
showDF(arp_lu_srs_product_df)

#######################################JOINING STARTS#################################

#Joining of product and product skus
print("Joining of product and product skus")
sprs_prd_prd_sku_df<- join(sprs_product_df,
                         sprs_product_sku_df,
                         sprs_product_df$PRD_IRL_NO==sprs_product_sku_df$SKU_PRD_IRL_NO)
print("Number of product and product skus")
#count(sprs_prd_prd_sku_df)
head(sprs_prd_prd_sku_df)

#Joining of product,product skus product core bridge skus
print("Joining of product,product skus product core bridge skus")
sprs_prd_sku_bridge_df <- join(sprs_prd_prd_sku_df,
                               sprs_product_core_bridge_sku_df,
                               sprs_prd_prd_sku_df$DIV_NO==sprs_product_core_bridge_sku_df$SRS_DIV_NO &
                               sprs_prd_prd_sku_df$ITM_NO==sprs_product_core_bridge_sku_df$SRS_ITM_NO &
                               sprs_prd_prd_sku_df$SKU_NO==sprs_product_core_bridge_sku_df$SRS_SKU_NO 
                              )
print("Number of product,product skus product core bridge skus")
#count(sprs_prd_sku_bridge_df)
head(sprs_prd_sku_bridge_df)

#Join forecast and location dataframes
print("Join forecast and location dataframes")
locn_frcst_df = join(sears_open_locn_df,
                     frcst_item_final_df, 
                     sears_open_locn_df$LOCN_NBR==frcst_item_final_df$LOCATION_ID)

#count(locn_frcst_df)
head(locn_frcst_df)

#Join Location_Forecast dataframes with product,sku and bridge on KSN_ID and SHC_ITM
print("Joining Location_Forecast dataframes with product,sku and bridge on KSN_ID and SHC_ITM")
locn_frcst_sprs_prd_sku_bridge_df= join(locn_frcst_df,
                                        sprs_prd_sku_bridge_df,
                                        locn_frcst_df$SHC_ITM==sprs_prd_sku_bridge_df$KSN_ID)
head(locn_frcst_sprs_prd_sku_bridge_df)
#count(locn_frcst_sprs_prd_sku_bridge_df)

#Join Location_Forecast_Product_sku_Bridge with Sears Lead Time")
print("Joining Location_Forecast_Product_sku_Bridge with Sears Lead Time")
locn_frcst_sprs_prd_sku_bridge_lead_time_df <- join(locn_frcst_sprs_prd_sku_bridge_df,
                                                    sears_lead_times_df,
                                                    sears_lead_times_df$Div==locn_frcst_sprs_prd_sku_bridge_df$DIV_NO &
                                                    sears_lead_times_df$Line==locn_frcst_sprs_prd_sku_bridge_df$LN_NO)

head(locn_frcst_sprs_prd_sku_bridge_lead_time_df)

sears_raw_data <- join(locn_frcst_sprs_prd_sku_bridge_lead_time_df,
                       arp_lu_srs_product_df,
                       locn_frcst_sprs_prd_sku_bridge_lead_time_df$DIV_NO==arp_lu_srs_product_df$DIV_NBR &
                       locn_frcst_sprs_prd_sku_bridge_lead_time_df$ITM_NO==arp_lu_srs_product_df$ITM_NBR )
head(sears_raw_data)
#count(sears_raw_data)

#######################################JOINING ENDS#################################

print("Casting output schema")
sears_raw_data <-withColumn(sears_raw_data,"LdTime", cast(sears_raw_data$Ldtime,"double"))
sears_raw_data <-withColumn(sears_raw_data,"SDLdTime", cast(sears_raw_data$SDLdTime,"double"))
sears_raw_data <-withColumn(sears_raw_data,"CALCMAPE", cast(sears_raw_data$CALCMAPE,"double"))
sears_raw_data <-withColumn(sears_raw_data,"SD95", cast(sears_raw_data$SD95,"double"))
sears_raw_data <-withColumn(sears_raw_data,"WEEK1", cast(sears_raw_data$WEEK1,"double"))
sears_raw_data <-withColumn(sears_raw_data,"WEEK2", cast(sears_raw_data$WEEK2,"double"))
sears_raw_data <-withColumn(sears_raw_data,"WEEK3", cast(sears_raw_data$WEEK3,"double"))
sears_raw_data <-withColumn(sears_raw_data,"WEEK4", cast(sears_raw_data$WEEK4,"double"))
sears_raw_data <-withColumn(sears_raw_data,"WEEK5", cast(sears_raw_data$WEEK5,"double"))
sears_raw_data <-withColumn(sears_raw_data,"WEEK6", cast(sears_raw_data$WEEK6,"double"))
sears_raw_data <-withColumn(sears_raw_data,"WEEK7", cast(sears_raw_data$WEEK7,"double"))
sears_raw_data <-withColumn(sears_raw_data,"WEEK8", cast(sears_raw_data$WEEK8,"double"))
sears_raw_data <-withColumn(sears_raw_data,"WEEK9", cast(sears_raw_data$WEEK9,"double"))
sears_raw_data <-withColumn(sears_raw_data,"WEEK10", cast(sears_raw_data$WEEK10,"double"))
sears_raw_data <-withColumn(sears_raw_data,"WEEK11", cast(sears_raw_data$WEEK11,"double"))
sears_raw_data <-withColumn(sears_raw_data,"WEEK12", cast(sears_raw_data$WEEK12,"double"))
sears_raw_data <-withColumn(sears_raw_data,"WEEK13", cast(sears_raw_data$WEEK13,"double"))

printSchema(sears_raw_data)

##############################FUNCTIONS START##############################

calculateEOQ<- function(weekMeasure,NATL_CST_PRC){
  EOQ_DC = sqrt((2*5*weekMeasure*52)/(0.2*(NATL_CST_PRC)))
  return (EOQ_DC)
}

calculateSafetyStock <- function(weekMeasure,CALCMAPE){
  safety_stock_DC <- sqrt(Lead_Time_DC/DAYS_PER_WEEK)*(SAFETY_STOCK_ZFACTOR*((CALCMAPE/100)*weekMeasure))
  return(safety_stock_DC)
}

reorderQty_safetyStock_OptimalInventory <- function(raw_data_df) {
  EOQ_DC_Week1 <- calculateEOQ(raw_data_df$WEEK1, raw_data_df$NATL_CST_PRC)
  EOQ_DC_Week2 <- calculateEOQ(raw_data_df$WEEK2, raw_data_df$NATL_CST_PRC)
  EOQ_DC_Week3 <- calculateEOQ(raw_data_df$WEEK3, raw_data_df$NATL_CST_PRC)
  EOQ_DC_Week4 <- calculateEOQ(raw_data_df$WEEK4, raw_data_df$NATL_CST_PRC)
  EOQ_DC_Week5 <- calculateEOQ(raw_data_df$WEEK5, raw_data_df$NATL_CST_PRC)
  EOQ_DC_Week6 <- calculateEOQ(raw_data_df$WEEK6, raw_data_df$NATL_CST_PRC)
  EOQ_DC_Week7 <- calculateEOQ(raw_data_df$WEEK7, raw_data_df$NATL_CST_PRC)
  EOQ_DC_Week8 <- calculateEOQ(raw_data_df$WEEK8, raw_data_df$NATL_CST_PRC)
  EOQ_DC_Week9 <- calculateEOQ(raw_data_df$WEEK9, raw_data_df$NATL_CST_PRC)
  EOQ_DC_Week10 <-calculateEOQ(raw_data_df$WEEK10, raw_data_df$NATL_CST_PRC)
  EOQ_DC_Week11 <-calculateEOQ(raw_data_df$WEEK11, raw_data_df$NATL_CST_PRC)
  EOQ_DC_Week12 <-calculateEOQ(raw_data_df$WEEK12, raw_data_df$NATL_CST_PRC)
  EOQ_DC_Week13 <-calculateEOQ(raw_data_df$WEEK13, raw_data_df$NATL_CST_PRC)
  
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
##############################FUNCTIONS ENDs##############################
outputSchema <- structType(
  structField("PRD_IRL_NO", "string"),
  structField("DIV_NO", "integer"),
  structField("LN_NO", "string"),
  structField("ITM_NO", "integer"),
  structField("SKU_NO", "integer"),
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
  structField("NATL_CST_PRC", "double"),
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
new_df <- repartition(sears_raw_data, col=sears_raw_data$LOCN_NBR, numPartitions=numOfPartitions)
final_df <- dapply(new_df,reorderQty_safetyStock_OptimalInventory,outputSchema)
print("dapply ends")
head(final_df)
write.df(final_df,paste("optimal-inventory-sears-dc",".csv",sep=""), source="com.databricks.spark.csv", "overwrite")
print("jobs ends")
print("Stopping SparkR Session")
sparkR.stop()

