#------------------------------------------
# Extract HES data
#
#------------------------------------------

# Author: VN
# dependency: None
#--------------------------
# Load packages
#--------------------------
library(sparklyr)
library(tidyverse)
library(zoo)

#--------------------------
# Set up the spark connection
#--------------------------

config <- spark_config() 
config$spark.dynamicAllocation.maxExecutors <- 30
config$spark.executor.cores <- 5
config$spark.executor.memory <- "20g"
config$spark.driver.maxResultSize <- "10g"
sc <- spark_connect(master = "yarn-client",
                    app_name = "R_Example",
                    config = config,
                    version = "2.3.0")



## function to extract relevant variables in all HES files

get_hes <- function(year){

  datafile = paste0('apc', '_hes_', year, '_std')

  hes_apc <- sdf_sql(sc, paste0("SELECT newnhsno_hes, dob_hes  FROM hospital_episode_statistics.", datafile))%>%
#  filter(  to_date(substring(admidate_hes , 1, 10), 'yyyy-MM-dd')>=
  #       to_date(paste0(as.character(round(year)),"-03-31")))%>%
  #select(-admidate_hes)%>%
  sdf_drop_duplicates()%>%
  mutate(year = year,
        type = "apc")
 
  datafile = paste0('ae', '_hes_', year, '_std')

  if (year < 2020){
  hes_ae <- sdf_sql(sc, paste0("SELECT newnhsno_hes, dob_hes FROM hospital_episode_statistics.", datafile))%>%
  sdf_drop_duplicates()%>%
  mutate(year = year,
        type="ae")
  }
  
  datafile = paste0('op', '_hes_', year, '_std')
  
  hes_op <- sdf_sql(sc, paste0("SELECT newnhsno_hes, dob_hes FROM hospital_episode_statistics.", datafile))%>%
  sdf_drop_duplicates()%>%
  mutate(year = year,
        type = "op")
  

  if (year < 2020){
    hes <- sdf_bind_rows(hes_apc, hes_ae, hes_op)
     
  }else{
    hes <- sdf_bind_rows(hes_apc, hes_op)
  }
    return(hes)
}


## extract for all files between 2011 and 2021

for(i in 2011:2021){
  hes <-  get_hes(i)
  print(i)
  assign(paste0("hes_", i), hes)
}


 hes <- sdf_bind_rows(hes_2011,
                     hes_2012,
                     hes_2013,
                     hes_2014,
                     hes_2015,
                     hes_2016,
                     hes_2017,hes_2018,hes_2019,hes_2020,
                     hes_2021)

hes <- hes %>%
    mutate(age = datediff(to_date(paste0(as.character(round(year+1)),"-03-31"), 'yyyy-MM-dd'),
                         to_date(substring(dob_hes, 1, 10), 'yyyy-MM-dd')))



## Delete table
sql <- paste0('DROP TABLE IF EXISTS cen_dth_gps.hes_phd_cohort_20230927')
DBI::dbExecute(sc, sql)

## Save table
sdf_register(hes , 'sdf_to_match')
sql <- paste0('CREATE TABLE cen_dth_gps.hes_phd_cohort_20230927 AS SELECT * FROM sdf_to_match')
DBI::dbExecute(sc, sql)
