#------------------------------------------
# Analyse the coverage of deaths included 
# in the PHDA 2011 Census cohort
#
#------------------------------------------

# Author: VN
# dependency: 1_extract_death_data.py

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

## read postcode lookup 

pcd <- sdf_sql(sc, paste0("SELECT * FROM ", postcode_lookup))%>%
  select(pcd,ctry,ru11ind,rgn,pcds, pcd2)%>%
  mutate(pcd = regexp_replace(pcd, " ", ""))

## read death data

deaths <-sdf_sql(sc, paste0("SELECT * FROM ", death_data)) %>%
  select(nhsno = NHSNO, dod=DOD,dodyr,dodmt, ageinyrs, sex, pcd = pcdr)%>%
  mutate(pcd = regexp_replace(pcd, " ", ""),
    dod = to_date(dod, 'yyyyMMdd' ))%>%
  filter(dod < to_date(paste0("2021-12-31"),  'yyyy-MM-dd'),
        dod >= to_date(paste0("2011-03-27"),  'yyyy-MM-dd'))%>%
  left_join(pcd, by="pcd")%>%
  mutate(linked_pcd = ifelse(!is.na(ctry),1,0))

## check linkage of death to postcode lookup

linked_counts <- deaths%>%
group_by(dodyr)%>%
summarise(n=n(),
         linked= sum(linked_pcd))%>%
collect() %>%
mutate(linkage_rate = linked /n )

print(linked_counts)


## read census data

df <- sdf_sql(sc, paste0("SELECT * FROM ", census_data))%>%
mutate(in_census = case_when(uresindpuk11_census == 1 & 
                             present_in_census == 1 & 
                             present_in_cenmortlink == 1 ~1,
                                     TRUE~0))%>%
select(nhsno = nhsno_deaths, in_census)

#grep("pc", colnames(deaths), value=T)

#----------------------------------------------------
#  derive number of deaths and coverage
#   by year,age and sex
#----------------------------------------------------

# number of deaths and coverage by year, age and sex
deaths_df <- deaths %>% 
        filter(ctry == "E92000001")%>%
     #filter(dod >= to_date(paste0("2011-07-01"),  'yyyy-MM-dd'))%>%
             left_join(df, by = "nhsno" )%>%
            group_by(ageinyrs, sex, dodyr, dodmt)%>%
            summarise(deaths_in_census = sum(in_census, na.rm =T),
                      deaths = n())%>%
            collect()

deaths_df <- deaths_df %>%
arrange(dodyr, sex, ageinyrs )%>%
mutate(coverage = deaths_in_census / deaths,
      dodym = as.yearmon(paste(dodyr, dodmt,"01", sep="-")))

write.csv(deaths_df , "Results/PHDA/deaths_coverage.csv")

#-----------------------------------
#  Produce coverage statistics
#-----------------------------------


deaths_df <- read.csv( "Results/PHDA/deaths_coverage.csv")%>%
mutate(coverage = deaths_in_census / deaths,
      dodym = as.yearmon(paste(dodyr, dodmt,"01", sep="-")))


## overall
coverage_all <- data.frame(Sex = "All",
                       agegrp = "All",
                       year= "All", 
                       deaths = sum(deaths_df$deaths),
                       deaths_in_census = sum(deaths_df$deaths_in_census, na.rm=T))%>%
    mutate(coverage = deaths_in_census /deaths)


## by year

deaths_by_year_df <- deaths_df %>%
    group_by(dodyr)%>%
summarise(deaths_in_census = sum(deaths_in_census, na.rm=T),
         deaths=sum(deaths,na.rm=T))%>%
mutate(coverage = deaths_in_census / deaths,
      Sex="All",
      agegrp="All",
      year = as.character(dodyr))


## by age groups, year and sex

deaths_by_age_year <- deaths_df %>%
    mutate(#age_grp = cut(age, breaks= c(0, 10, 30, 60seq(0,90, 5), Inf), right=F),
       age_grp = cut(ageinyrs, breaks= c(seq(0,90, 10), Inf), 
                     labels= c("0-9", "10-19", "20-29",
                             "30-39", "40-49", "50-59",
                             "60-69", "70-79", "80-89", "90+"),
                     right=F))%>%
group_by(sex, dodyr, age_grp ) %>%
summarise(deaths_in_census = sum(deaths_in_census, na.rm=T),
         deaths=sum(deaths,na.rm=T))%>%
mutate(coverage = deaths_in_census / deaths)


# by age groups and year

deaths_by_age_sex <- deaths_by_age_year%>%
  filter(sex < 3) %>%
  mutate(Sex = ifelse(sex == 1, "Male", "Female"))%>%
  group_by(Sex,  age_grp ) %>%
  summarise(deaths_in_census = sum(deaths_in_census, na.rm=T),
           deaths=sum(deaths,na.rm=T))%>%
  mutate(coverage = deaths_in_census / deaths)%>%
  mutate(dodyr = "All")

deaths_by_sex <- deaths_by_age_sex%>%
  group_by(Sex) %>%
  summarise(deaths_in_census = sum(deaths_in_census, na.rm=T),
           deaths=sum(deaths,na.rm=T))%>%
  mutate(coverage = deaths_in_census / deaths)%>%
  mutate(year = "All", agegrp = "All")

coverage <- bind_rows(coverage_all,
                      deaths_by_sex,
                      mutate(deaths_by_year_df, dodyr=as.character(dodyr) )%>%
                   
                             deaths_by_age_sex )
write.csv(coverage , paste0(results, "/coverage_summary_df.csv"))


#-----------------------------------
#  draw graphs
#-----------------------------------

ggplot(data = deaths_by_age_sex ,
      aes(x = age_grp, y = coverage * 100, fill = Sex))+
  geom_col(position="dodge", width=0.7)+
  geom_label(aes(label=round(coverage*100, 1)),
             position= position_dodge(width = 1.2),
            size=3)+
  labs(y="Perc. of deaths", x="Age")+
  theme_bw()+
  theme(legend.position="top")+
  scale_fill_brewer(palette="Accent")



ggsave(paste0(results, "/death_coverage_by_age_sex.png"), width = 16/9*5, height=5)



# by year - month
deaths_by_year_df <- deaths_df %>%
    group_by(dodym, dodyr, dodmt)%>%
  summarise(deaths_in_census = sum(deaths_in_census, na.rm=T),
           deaths=sum(deaths,na.rm=T))%>%
  mutate(coverage = deaths_in_census / deaths)%>%
  ungroup()

ggplot(data = filter(deaths_by_year_df , dodyr >= 2012 | dodmt != 3),
      aes(x = dodym, y = coverage*100))+
  geom_line()+
  labs(y="Perc. of deaths", x="Year")+
  theme_bw()
  ggsave("Results/PHDA/coverage_death_by_year.png")

write.csv(deaths_by_year_df , paste0(results, "\deaths_coverage_by_year_month.csv"))


deaths_by_year_sex_df <- deaths_df %>%
    group_by(sex, dodyr)%>%
  summarise(deaths_in_census = sum(deaths_in_census, na.rm=T),
           deaths=sum(deaths,na.rm=T))%>%
  mutate(coverage = deaths_in_census / deaths)

# by year

deaths_by_year_df <- deaths_df %>%
    group_by(dodyr)%>%
  summarise(deaths_in_census = sum(deaths_in_census, na.rm=T),
           deaths=sum(deaths,na.rm=T))%>%
  mutate(coverage = deaths_in_census / deaths)

  ggplot(data = deaths_by_year_df ,
        aes(x = round(dodyr,0), y = coverage))+
  geom_line()+
  labs(y="Perc. of deaths", x="Year")+
  theme_bw()
  ggsave(paste0(results, "/coverage_death_by_year.png")

