#------------------------------------------
# Analyse the coverage of the population
# included in the PHDA 2011 Census cohort
#
#------------------------------------------

# Author: VN

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
# specify dataset version
dataset_version = '20220817'

# read linked dataset
df <- sdf_sql(sc, paste0("SELECT * FROM ", census_data))



# add information about linkage
linkage_info <-  df_sql(sc, paste0("SELECT * FROM ", linkage_data))%>%
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


#-------------------------------------
# get number of participants in PHDA 
# alive at mid point of every year
#--------------------------------------


census_pop_by_year <- purrr::map_dfr(2011:2021, function(year){
  
  year_str= as.character(year)
  
census_pop <- census %>%
              filter(dod > to_date(paste0(year_str,"-06-30"),  'yyyy-MM-dd') | is.na(dod))%>%
 mutate(age = floor(datediff( to_date(paste0(year_str,"-06-30"),  'yyyy-MM-dd'),
                                           dob) / 365))%>%
              group_by(age, sex, country)%>%
              count()%>%
              collect()%>%
              mutate(year = year)
   print(paste0(year, ":", sum(census_pop$n)))
return(census_pop)
 
})

census_pop_by_year <- census_pop_by_year %>%
                     arrange(year, country, sex, age)


write.csv(census_pop_by_year, paste0(results, "/census_pop.csv"))


#-------------------------------------
# Compare number of participants in PHDA 
# alive at mid point of every year to
# mid year estimates
#--------------------------------------
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
results="Results"

## read participant number

census_pop <-   read.csv(paste0(results,"/census_pop.csv"))%>%
  select(-X) %>%
  mutate(age = ifelse(age > 90, 90, age))%>%
  filter(year < 2021)%>%
  group_by(sex, year, country, age)%>%
  summarise(census = sum(n)) 

## read data on population estimates

est_pop <- read.csv(paste0(results,"/pop_estimates.csv"))

## link the two files
linked <- census_pop %>%
  mutate(sex = as.numeric(sex))%>%
            left_join(est_pop, by=c("age", "sex", "year", "country"))

  nrow(linked) == nrow(census_pop)

# check
sum(is.na(linked$population)) == 0


## produce coverage estimates

## By year

# by sex
coverage_sex <- linked %>%
filter(country =="England")%>%
mutate(as.factor(year))%>%
group_by(year, sex)%>%
summarise(census = sum(census),
         population = sum(population),
         coverage = census/population*100)%>%
mutate(age_grp="All")%>% 
mutate(Sex = ifelse(sex == 1, "Male", "Female"))


# all
coverage <- coverage_sex %>% 
group_by(year)%>%
summarise(census = sum(census),
         population = sum(population),
         coverage = census/population*100)%>%
mutate(age_grp="All")



ggplot(data = coverage,
      aes(x = round(year,0), y = coverage))+
geom_line()+
labs(y="Perc. of population", x="Year")+
theme_bw()+
ylim(0,100)
ggsave(paste0(results,"/coverage_by_year.png"))

write.csv(coverage_ew,paste0(results,"/coverage_by_year.csv" ))

## England by age group

coverage_age_sex <- linked %>%
filter(country =="England")%>%
mutate(#age_grp = cut(age, breaks= c(0, 10, 30, 60seq(0,90, 5), Inf), right=F),
       age_grp = cut(age, breaks= c(seq(0,90, 10), Inf), 
                     labels= c("0-9", "10-19", "20-29",
                             "30-39", "40-49", "50-59",
                             "60-69", "70-79", "80-89", "90+"),
                     right=F))%>%
group_by(year, age_grp, sex)%>%
summarise(census = sum(census),
         population = sum(population),
         coverage = census/population*100)%>% 
mutate(Sex = ifelse(sex == 1, "Male", "Female"))

coverage_age <- coverage_age_sex %>%
group_by(year, age_grp)%>%
summarise(census = sum(census),
         population = sum(population),
         coverage = census/population*100)

coverage_age <- bind_rows(coverage_age, coverage)
coverage_age_sex <-bind_rows(coverage_age_sex, coverage_sex)


#-------------------------------------
# Draw graphs
#--------------------------------------



## coverage by age over time
ggplot(data = filter(coverage_age, age_grp !="0-9"),
      aes(x = as.integer(year), y = coverage, colour=age_grp))+
geom_line()+
labs(y="Perc. of population", x="Year")+
theme_bw()+
scale_colour_brewer(palette="Spectral")+
scale_x_continuous(breaks = ~ axisTicks(., log = FALSE))+
ylim(50,100)

ggsave(paste0(results,"/coverage_by_age.png"))

### Figure 1 
## A coverage by age in 2011
g_A <- ggplot(data = filter(coverage_age, year == 2011),
      aes(x = age_grp, y = coverage))+
      geom_col()+
      geom_label(aes(label=round(coverage, 1)))+
      labs(title ="A - By age group", y="Perc. of population in mid 2011", x="Age")+
      theme_bw()



#ggsave(paste0(results,"/coverage_by_age_2011.png"), width = 16/9*5, height=5)


write.csv(coverage_age,paste0(results,"/coverage_by_year.csv" ))


## coverage by age in 2011 by sex
g_B <- ggplot(data = filter(coverage_age_sex, year == 2011),
      aes(x = age_grp, y = coverage, fill = Sex))+
geom_col(position="dodge", width=0.7)+
geom_label(aes(label=round(coverage, 1)),
           position= position_dodge(width = 1.2),
          size=3)+
labs(title ="B - By age group and sex", y="Perc. of population in mid 2011", x="Age")+
theme_bw()+
theme(legend.position="top")+
scale_fill_brewer(palette="Greys")

ggpubr::ggarrange(g_A, g_B, nrow = 2)

ggsave(paste0(results,"/Figure_2.jpg"), width = 8, height=12, dpi=300)

