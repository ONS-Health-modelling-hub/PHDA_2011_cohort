#------------------------------------------
# Analyse quality of coding of
# diagnosis and procedure
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


# --------------------#
#      Function       #
# --------------------#

## function to estimate missingness in diag and procedure code by year in APC and OP, for a given year

get_missing_diag_hes<- function(year){
  
  if (year >= 2020){
    filetype = '_std'
    diag_var = 'diag_4_01_hes'
    procedure = 'operstat_hes'
    
    
  }else{
    filetype = '_extra_std'
    diag_var = 'diag_4_01'
    procedure = 'operstat'

  }
  datafile = paste0('op', '_hes_', year, filetype)
  hes_op <- sdf_sql(sc, paste0("SELECT ",diag_var, ",",procedure ," FROM hospital_episode_statistics.", datafile))%>%
  mutate(year = year,
        type = "op",
        missing_diag = ifelse(is.na(!!sym(diag_var)) | !!sym(diag_var) == 'R69X',1, 0),
        missing_procedure = ifelse(!!sym(procedure) == 1, 1, 0))%>%
  summarise(n_missing_diag = sum(missing_diag),
            n_missing_op = sum(missing_procedure),
         n=n())%>% 
mutate(missing_rate_diag = n_missing_diag/n,
       missing_rate_op =  n_missing_op/n,
      year = year,
        type = "op")%>%
collect()

  datafile = paste0('apc', '_hes_', year, filetype)
  hes_apc <- sdf_sql(sc, paste0("SELECT ",diag_var , ",",procedure ," FROM hospital_episode_statistics.", datafile))%>%
  mutate(year = year,
        type = "apc",
    missing_diag = ifelse(is.na(!!sym(diag_var)) | !!sym(diag_var) == 'R69X',1, 0),
        missing_procedure = ifelse(!!sym(procedure) == 1, 1, 0))%>%
  summarise(n_missing_diag = sum(missing_diag),
            n_missing_op = sum(missing_procedure),
         n=n())%>% 
  mutate(missing_rate_diag = n_missing_diag/n,
       missing_rate_op =  n_missing_op/n,
      year = year,
        type = "apc")%>%
  collect()


  hes <-rbind(hes_apc, hes_op)
return(hes)
}

# --------------------#
# Call function       #
# --------------------#


for(i in 2011:2021){
  hes <-  get_missing_diag_hes(i)
  print(i)
  assign(paste0("hes_", i), hes)
}


 hes <- rbind(hes_2011,
                     hes_2012,
                     hes_2013,
                     hes_2014,
                     hes_2015,
                     hes_2016,
                     hes_2017,hes_2018,hes_2019,hes_2020,
                     hes_2021)

print( as.data.frame(hes))


write.csv( hes  , paste0(results, "/hos_diag_missing.csv"))

hes %>% 
group_by(type) %>%
summarise(valid_rate_diag = 1 - sum(n_missing_diag) / sum(n),
         valid_rate_proc = 1 - sum(n_missing_op) / sum(n))


