#------------------------------------------
#
# Produce sample flow for participants 
# included in the PHDA 2011 cohort
# and derive information on how 
# they were linked to the PR
#
#------------------------------------------

# Author:VN

#--------------------------
# Load packages
#--------------------------
library(sparklyr)
library(tidyverse)

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

# read linked dataset
df <- sdf_sql(sc, paste0("SELECT * FROM ", census_data))


#--------------------------
# Produce sample flow
#--------------------------
# filter only census respondents
census <- df %>%
          filter( present_in_census == 1)%>%
          mutate(country = case_when(region_code_census == "W92000004" ~ "Wales",
                                     region_code_census != "W92000004" ~ "England" ))


n_census <- census%>%
            group_by(country) %>% 
            count() %>%
            collect()

n_real_resp = census%>%
filter(present_in_cenmortlink == 1)%>%
group_by(country) %>% 
count() %>%
collect()


n_real_resp_ur = census%>%
filter(present_in_cenmortlink == 1, uresindpuk11_census == 1)%>%
group_by(country) %>% 
count() %>%
collect()

n_linked = census%>% 
filter(uresindpuk11_census == 1, present_in_cenmortlink == 1, cen_pr_flag == 1)%>%
group_by(country) %>% 
count() %>%
collect()


n_census
n_real_resp
n_linked 
 

res <- data.frame(country = n_census$country,
                  n_census= n_census$n,
                  n_real_resp =n_real_resp$n,
                  n_real_resp_ur = n_real_resp_ur$n,
                  n_linked = n_linked$n 
                  )

write.csv(res, paste0(results, "/flow_chart.csv"))



#-------------------------------------
# produce information on linkage type
#----------------------------------
### by type of linkage ####
# add information about linkage
linkage_info <-  sdf_sql(sc, paste0("SELECT * FROM ", linkage_data))%>%
select(census_person_id,key_seq, priority, matching_method, prob_score )


# filter only census respondents
census <- df %>%
          filter(uresindpuk11_census == 1,
                 present_in_census == 1,
                 present_in_cenmortlink == 1,
                cen_pr_flag ==  1)%>%
          mutate(dob = to_date(dob_census,  'yyyy-MM-dd'),
                 dod = to_date(dod_deaths, 'yyyy-MM-dd'),
                 country = case_when(region_code_census == "W92000004" ~ "Wales",
                                     region_code_census != "W92000004" ~ "England" ))%>%
  select(census_person_id, age = age_census, sex = sex_census, dob,dod, dodyr = dodyr_deaths, country, region_code_census, final_nhs_number)%>%
  left_join(linkage_info, by="census_person_id")


n_priority <- census %>%
filter(country == "England")%>%
            group_by( priority)%>%
            count()%>%
            collect()%>%
            arrange(priority)

write.csv(n_priority , paste0(results,"/n_linkage_type.csv"))



n_key_seq <- census %>%
filter(country == "England")%>%
            group_by(key_seq)%>%
            count()%>%
            collect()

write.csv(n_key_seq , paste0(results,"/n_linkage_type_matchkey.csv"))