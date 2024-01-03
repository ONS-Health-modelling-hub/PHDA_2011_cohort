
#------------------------------------------
# Analyse the coverage of hospitalised patients
# in the PHDA 2011cohort
#
#------------------------------------------

# Author: VN
# dependency: 2_extract_hes.py

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


#--------------------------
# Read sparks dataframe
#--------------------------

## read HES data ##
hes <- sdf_sql(sc, paste0("SELECT * FROM ", hes_data)) 


# missing seems to be set to 1799-12-31
# for now, let's just select the youngest age

hes <- sdf_sql(sc, paste0("SELECT * FROM ", hes_data)) %>%
  group_by(year, newnhsno_hes, type)%>%
  summarise(age = floor(min(as.numeric(age/365))))%>%
  ungroup()


hes_gr_head <- hes %>%
  head(n=100)%>%
  collect()


##  number in hes by type

n_hes <- hes %>%
  select(newnhsno_hes, type)%>%
filter(!is.na(newnhsno_hes))%>%
 sdf_drop_duplicates()%>%
  group_by(type)%>%
count()%>%
collect()


## read Census data


df <-  sdf_sql(sc, paste0("SELECT * FROM ", census_data))%>%
filter( region_code_census != "W92000004")%>%
  mutate(in_census = case_when(uresindpuk11_census == 1 & 
                               present_in_census == 1 & 
                               present_in_cenmortlink == 1 ~1,
                                       TRUE~0))%>%
  select(newnhsno_hes,hes_nhsno,final_nhs_number, in_census)  %>%
  filter(in_census == 1)%>%
  mutate(nhsno = final_nhs_number)#ifelse(is.na(newnhsno_hes), hes_nhsno, newnhsno_hes))

## link to hes

hes_link <- hes %>%
filter(!is.na(newnhsno_hes))%>%
     left_join(df, by=c('newnhsno_hes' = 'nhsno'))%>%
  mutate(in_census = ifelse(is.na(in_census),0, in_census))


## overall coverage
overall_coverage <-  hes_link %>%
select(newnhsno_hes, type, in_census)%>%
 sdf_drop_duplicates()%>%
  group_by(type)%>%
  summarise(hosp_in_census = sum(in_census, na.rm =T),
            hosp = n())%>%
mutate( coverage = hosp_in_census / hosp )%>%
collect()

write.csv(overall_coverage , paste0(results, "/overall_coverage_hosp.csv"))


## Coverage 11+
overall_coverage_11 <-  hes_link %>%
mutate(age_11 = ifelse(age > 10, 1, 0)) %>%
select(newnhsno_hes, type, in_census, age_11)%>%
 sdf_drop_duplicates()%>%
  group_by(type, age_11)%>%
  summarise(hosp_in_census = sum(in_census, na.rm =T),
            hosp = n())%>%
mutate( coverage = hosp_in_census / hosp )%>%
collect()

write.csv(overall_coverage_11 , paste0(results, "/overall_coverage_hosp_11.csv"))

#--------------------------------------------------------------------------------#