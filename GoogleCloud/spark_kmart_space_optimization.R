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
k= 1+(3.322*log10(N_SS_WEEKS))

########################### INTIALIZE END###########################################

#Load Forecast Item final
frcst_item_final_df <- read.df(paste(classpath_data_source,"aat_tbls.frcst_item_final.csv*.gz",sep=""), source = "com.databricks.spark.csv", delimiter="|")
colnames(frcst_item_final_df) <- c("FISCAL_WK","MODEL_TYPE","SHC_DVSN_CAT_SUBCAT_NO","SHC_ITM","LOCATION_ID","SEAS_CD","SAS_SIZE_PROFILE_ID","SIZE_TYPE_CD","C_THRESHOLD","PVALUE","WEEK1","WEEK2","WEEK3","WEEK4","WEEK5","WEEK6","WEEK7","WEEK8","WEEK9","WEEK10","WEEK11","WEEK12","WEEK13","TOTAL","CALCMAPE","SD95","SD80")
head(frcst_item_final_df)


#Load Master Location
print("load master location")
extract_mstr_locn_df <- read.df(paste(classpath_data_source,"dataview.locn.txt",sep=""), source = "com.databricks.spark.csv", delimiter="|")
#mstr_locn_df <- as.data.frame(extract_mstr_locn_df[,c(1,3)], drop=false)
#head(extract_mstr_locn_df)
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

#Load KSN
print("Load KSN table")
ksn_df <- read.df(paste(classpath_data_source,"product.ksn_tbl.*gz",sep=""),  source = "com.databricks.spark.csv", delimiter="|")
ksn_df <-select(ksn_df,ksn_df$`_c0`,ksn_df$`_c2`,ksn_df$`_c5`)
colnames(ksn_df) <- c("KSN_ID","DVSN_NBR","CATG_NBR")
ksn_df <-withColumn(ksn_df,"KSN_ID", cast(ksn_df$KSN_ID,"integer"))
ksn_df <-withColumn(ksn_df,"DVSN_NBR", cast(ksn_df$DVSN_NBR,"integer"))
ksn_df <-withColumn(ksn_df,"CATG_NBR", cast(ksn_df$CATG_NBR,"integer"))
head(ksn_df)

######################JOINING STARTS********************************************************************

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

#Join with Kmart Lead Time
print("Join with Kmart Lead Time")
kmart_raw_data = join(frcst_item_location_ksn_df,
                              kmart_lead_times_df,
                              frcst_item_location_ksn_df$DVSN_NBR == kmart_lead_times_df$Div &
                              frcst_item_location_ksn_df$CATG_NBR == kmart_lead_times_df$Cat
                              )
head(kmart_raw_data)
kmart_raw_data <- drop(kmart_raw_data,c("KSN_ID","Div","Cat"))
#count(kmart_raw_data)
#****************************Joining Ends***************************#
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

printSchema(kmart_raw_data)

##############################FUNCTIONS STARTS##############################

calculateReorderQty <- function(weekMeasure,LdTime){
   re_order_qty <-weekMeasure*(LdTime/DAYS_PER_WEEK)
   return (re_order_qty)
}

calculateSafetyStock <- function(weekMeasure,LdTime,CALCMAPE,SDLdTime){
  safety_stock<-sqrt((LdTime/DAYS_PER_WEEK)*((CALCMAPE/(100*SAFETY_STOCK_ZFACTOR))^2)+(SDLdTime/DAYS_PER_WEEK)^2)*(SAFETY_STOCK_ZFACTOR*weekMeasure)
  return(safety_stock)
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
   
   re_ss_oo_df$OR1<-0
   re_ss_oo_df$OR2<-0
   re_ss_oo_df$OR3<-0
   re_ss_oo_df$OR4<-0
   re_ss_oo_df$OR5<-0
   re_ss_oo_df$HighestSpaceCapacity<-0
   
   for(i in 1:nrow(re_ss_oo_df)){
     re_ss_oo_df[i,"PcMax"] <- max(re_ss_oo_df[i,"OptimalInventory_1"],
                                   re_ss_oo_df[i,"OptimalInventory_2"],
                                   re_ss_oo_df[i,"OptimalInventory_3"],
                                   re_ss_oo_df[i,"OptimalInventory_4"],
                                   re_ss_oo_df[i,"OptimalInventory_5"],
                                   re_ss_oo_df[i,"OptimalInventory_6"],
                                   re_ss_oo_df[i,"OptimalInventory_7"],
                                   re_ss_oo_df[i,"OptimalInventory_8"],
                                   re_ss_oo_df[i,"OptimalInventory_9"],
                                   re_ss_oo_df[i,"OptimalInventory_10"],
                                   re_ss_oo_df[i,"OptimalInventory_11"],
                                   re_ss_oo_df[i,"OptimalInventory_12"],
                                   re_ss_oo_df[i,"OptimalInventory_13"]
                                   )
     re_ss_oo_df[i,"PcMin"] <-min(re_ss_oo_df[i,"OptimalInventory_1"],
                                  re_ss_oo_df[i,"OptimalInventory_2"],
                                  re_ss_oo_df[i,"OptimalInventory_3"],
                                  re_ss_oo_df[i,"OptimalInventory_4"],
                                  re_ss_oo_df[i,"OptimalInventory_5"],
                                  re_ss_oo_df[i,"OptimalInventory_6"],
                                  re_ss_oo_df[i,"OptimalInventory_7"],
                                  re_ss_oo_df[i,"OptimalInventory_8"],
                                  re_ss_oo_df[i,"OptimalInventory_9"],
                                  re_ss_oo_df[i,"OptimalInventory_10"],
                                  re_ss_oo_df[i,"OptimalInventory_11"],
                                  re_ss_oo_df[i,"OptimalInventory_12"],
                                  re_ss_oo_df[i,"OptimalInventory_13"]
                               )
    
     re_ss_oo_df[i,"RI"] <- (re_ss_oo_df[i,"PcMax"]-re_ss_oo_df[i,"PcMin"])/k
     re_ss_oo_df[i,"RM1"] <- re_ss_oo_df[i,"PcMin"] + (re_ss_oo_df[i,"RI"]-1)
     re_ss_oo_df[i,"RM2"] <- re_ss_oo_df[i,"RM1"]+ re_ss_oo_df[i,"RI"]
     re_ss_oo_df[i,"RM3"] <- re_ss_oo_df[i,"RM2"]+ re_ss_oo_df[i,"RI"]
     re_ss_oo_df[i,"RM4"] <- re_ss_oo_df[i,"RM3"]+ re_ss_oo_df[i,"RI"]
     re_ss_oo_df[i,"RM5"] <- re_ss_oo_df[i,"PcMax"]
     
     for (wk in 1:13) {
       OptimalInventory_Lbl <- paste0("OptimalInventory_", wk)
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
     }
     
   #Determine the Highest Capacity
     maxOcValue<-0
     for(wkNbr in 1:5){
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
  
  re_ss_oo_df <- cbind(
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
  
  maxMinOccurenceAndHighest_df <- calculateHighestSpaceCapacity(re_ss_oo_df)
  return(maxMinOccurenceAndHighest_df)
}


##############################FUNCTIONS END##############################

outputSchema <- structType(
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
  structField("DVSN_NBR", "integer"),
  structField("CATG_NBR", "integer"),
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
  structField("OptimalInventory13", "double"),
  structField("PcMax", "double"),
  structField("PcMin", "double"),
  structField("RI", "double"),
  structField("RM1", "double"),
  structField("RM2", "double"),
  structField("RM3", "double"),
  structField("RM4", "double"),
  structField("RM5", "double"),
  structField("OR1", "double"),
  structField("OR2", "double"),
  structField("OR3", "double"),
  structField("OR4", "double"),
  structField("OR5", "double"),
  structField("HighestSpaceCapacity", "double")
)

print("dapply starts")
#partitionCount <- count(distinct(select(kmart_raw_data,"LOCATION_ID")))
new_df <- repartition(kmart_raw_data, col=kmart_raw_data$LOCATION_ID, numPartitions=numOfPartitions)
new_df <- dapply(new_df,reorderQty_safetyStock_OptimalInventory,outputSchema)
persist(new_df, "MEMORY_AND_DISK")
#head(final_df)
print("dapply ends")
write.df(new_df,paste("optimal-space-optimization-kmart",".csv",sep=""), source="com.databricks.spark.csv", "overwrite")
print("jobs ends")
print("Stopping SparkR Session")
sparkR.stop()
