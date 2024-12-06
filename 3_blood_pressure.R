### PART 3 #################################################

###>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>###
###  Project: AEMR-US OMOP adult Hypertension Phenotype  ###
###                                                      ###
###         PART 3: Blood pressure codes                 ###
###    BP inclusion criteria; High BP calculations       ###
###                                                      ###
###           Data source: IQVIA AEMR-US OMOP            ###
###   Script Author: Siran He, CDC |  Jul-Aug 2023       ###
###   BP portions were adapted from Dr.Weng's script     ###
###  Modified for DSU Team BP Commanders | Dec 2024      ### 
###                                                      ###
###>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>###


# Step 1. Install useful packages ----------------------------------------------------------------

pacman::p_load(
  DBI, #ODBC connection package
  skimr,
  curl,
  rio,
  dplyr,
  janitor,
  lubridate,
  here,
  stringr,
  sqldf,
  tidyverse,
  data.table # for more efficient subsetting (for last few steps below)
  # gtsummary,
  # flextable,
  # growthcleanr
)

install.packages("data.table") # seems data.table recently had update, if needed, run these 2 lines
library(data.table)



# Step 2. Import clean data for analyses--------------------------------------------------

## NOTE:
  # The analyst could start by using this saved data set (already de-duplicated, and excluded pregnancy, ESRD, care)
  # Or, could choose to run PART 2 script (data_importing_cleaning) if need to perform quality control

  # slower import: import(here("data","clean", "all_pop_clean.csv"))
  
  # faster import:
    all_pop_clean  <- read.csv("C:\\Users\\tdu4\\weighting_project\\data\\clean\\all_pop_clean.csv")
    # all_pop_clean3 <- read.csv("C:\\Users\\tdu4\\weighting_project\\data\\clean\\all_pop_clean3.csv")
    # all_pop_clean4 <- read.csv("C:\\Users\\tdu4\\weighting_project\\data\\clean\\all_pop_clean4.csv")
    # all_pop_merged3 <- read.csv("C:\\Users\\tdu4\\weighting_project\\data\\clean\\all_pop_merged3.csv")
    # all_pop_merged5 <- read.csv("C:\\Users\\tdu4\\weighting_project\\data\\clean\\all_pop_merged5.csv")

    
    # sbp_query <- read.csv("C:\\Users\\tdu4\\weighting_project\\data\\clean\\sbp_query.csv")
    # dbp_query <- read.csv("C:\\Users\\tdu4\\weighting_project\\data\\clean\\dbp_query.csv")

    # sbp_clean2 <- read.csv("C:\\Users\\tdu4\\weighting_project\\data\\clean\\sbp_clean2.csv")
    # dbp_clean2 <- read.csv("C:\\Users\\tdu4\\weighting_project\\data\\clean\\dbp_clean2.csv")



    
    
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#  
    # Load the database name ---
    
    #' [This is the actual full dataset with 12M + patients]
    #  db_name<-("AEMR_2023MAIN_WITH2022_06SEP2024_202737_699.DBO")
    
    #' [For practice purpose, use this mini dataset with 1000 patients]
    db_name<-("RANDOM1000_AEMR_2023MAIN_WITH2022_02DEC2024_201506_513.DBO")
    
    # Set strings for measurement periods ---
    year_string_1<-("2023")  # main study period
    year_string_2<-("2022, 2023") # study period + one year lookback for BP check


# Step 3. Query SBP & DBP directly from Snowflake PDE data-----------------------------------------------------
    
  # Systolic blood pressure query:
    sbp_query <-
      gsub("\n", "", 
           paste(
             paste0("SELECT 
                         PATIENT_LINKAGE
                         ,MEASUREMENT_DATE
                         ,MEASUREMENT_CONCEPT_ID
                         ,MEASUREMENT_CONCEPT_DESC
                         ,VALUE_AS_NUMBER
                         ,UNIT_CONCEPT_DESC
                      FROM 
                         ",db_name,".MEASUREMENT
                      WHERE 
                         MEASUREMENT_CONCEPT_ID IN (4152194, 3004249, 4232915, 3018586)                       
                      AND
                         YEAR(DRUG_EXPOSURE_START_DATE) IN (",year_string_2,")
                      AND
                         UNIT_CONCEPT_ID = 8876                 
                      "
             )))
    
    
  # Diastolic blood pressure query:
    dbp_query <-
      gsub("\n", "", 
           paste(
             paste0("SELECT 
                         PATIENT_LINKAGE
                         ,MEASUREMENT_DATE
                         ,MEASUREMENT_CONCEPT_ID
                         ,MEASUREMENT_CONCEPT_DESC
                         ,VALUE_AS_NUMBER
                         ,UNIT_CONCEPT_DESC
                      FROM 
                         ",db_name,".MEASUREMENT
                      WHERE 
                         MEASUREMENT_CONCEPT_ID IN (4154790, 3012888, 4248524, 3034703)                       
                      AND
                         YEAR(DRUG_EXPOSURE_START_DATE) IN (",year_string_2,")
                      AND
                         UNIT_CONCEPT_ID = 8876
                      "
             )))
    

## As needed, export these two datasets because the steps above are a bit slow:
    # export(sbp_query, here("data","clean","sbp_query.csv"))
    # export(dbp_query, here("data","clean","dbp_query.csv"))

    
    
#' [Note to BP Commanders: lots of room for improvement in the scripts below. While you review this file, please think of ways to optimize the process]    
#' [Tidyverse tend to be a bit slow with large data, so I switched back and forth with base R or data.table as needed]
        
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#  
    
# Step 4. Apply BP inclusion criteria in the denominator ----------------------

## 4.a. Remove biologically implausible values (source: Dr. Larry Sperling) ----
   
   sbp_query$SBP <- sbp_query$VALUE_AS_NUMBER
     sbp_clean1 <- sbp_query %>% 
        filter(SBP %in% (30:300) & !is.na(SBP))

   dbp_query$DBP <- dbp_query$VALUE_AS_NUMBER
     dbp_clean1 <- dbp_query %>% 
       filter(DBP %in% (20:150) & !is.na(DBP))
    
# very slow codes:
   # sbp_clean1 <- sbp_query %>% 
   #    mutate(SBP = VALUE_AS_NUMBER) %>% 
   #    filter(SBP %in% (30:300) & !is.na(SBP))
   #  
   #  
   # dbp_clean1 <- dbp_query %>% 
   #    mutate(DBP = VALUE_AS_NUMBER) %>% 
   #    filter(DBP %in% (20:150) & !is.na(DBP))
 

## 4.b. Average same-day values for SBP & DBP, respectively ----

# very slow codes:
     
   sbp_clean2 <- sbp_clean1 %>%
     group_by(PATIENT_LINKAGE, MEASUREMENT_DATE) %>%
     summarise(SBP_avg = round(mean(SBP, na.rm = T),1))  # round the mean value to 1 decimal point

   dbp_clean2 <- dbp_clean1 %>%
     group_by(PATIENT_LINKAGE, MEASUREMENT_DATE) %>%
     summarise(DBP_avg = round(mean(DBP, na.rm = T),1))

  
 # QC: check data from 1 random patient with multiple values on the same day to see if avg was correct
   
   # sbp_clean1 %>%
   #   filter(PATIENT_LINKAGE == "++/7ycH1Qxq0AuAOkFVcjg==" &
   #          MEASUREMENT_DATE == "2020-06-08") %>%
   #   select(PATIENT_LINKAGE, MEASUREMENT_DATE, SBP)

   
  # Export these two datasets because the steps above are a bit slow:
    # free up space
      remove(sbp_query)
      remove(dbp_query)
   
    #export large intermediate datasets
      export(sbp_clean2, here("data","clean","sbp_clean2.csv"))
      export(dbp_clean2, here("data","clean","dbp_clean2.csv"))
   
   

## 4.c. Inclusion: must have 1 complete BP in the past 2 years (2021-2022) ----
     
# Join SBP and DBP files for same-day pairing 
   
    sbp_clean2<- sbp_clean2 %>% ungroup()
    dbp_clean2<- dbp_clean2 %>% ungroup()
    
    both_bp <- sbp_clean2 %>% 
       inner_join(dbp_clean2, by = c("PATIENT_LINKAGE"="PATIENT_LINKAGE", 
                                    "MEASUREMENT_DATE"="MEASUREMENT_DATE"))
   
   all_pop_clean2 <- all_pop_clean %>% 
     left_join(both_bp, by = "PATIENT_LINKAGE")
   
# No missing in BP by measurement day and by patient_linkage
   all_pop_clean3 <- all_pop_clean2 %>% 
     group_by(PATIENT_LINKAGE, MEASUREMENT_DATE) %>% 
     filter(!is.na(SBP_avg) & !is.na(DBP_avg))

# will use this subset for hypertension e-phenotype high BP identification (one row per each patient's measurement date)
   all_pop_clean3 <- all_pop_clean3 %>% ungroup()
   

# will use this one to count patients after BP denominator criteria (one row per patient)   
   all_pop_clean4 <- all_pop_clean3 %>% 
     distinct(PATIENT_LINKAGE, .keep_all = TRUE) %>% 
     select(PATIENT_LINKAGE, YEAR_OF_BIRTH, age, sex, race, state, zip3)
 
   
   
 # Export these datasets because the steps above are a bit slow:
   export(all_pop_clean3, here("data","clean","all_pop_clean3.csv"))
   export(all_pop_clean4, here("data","clean","all_pop_clean4.csv"))
  

   
 # Quality checks:
   # check one patient (+++3bBI01w/HnR669dgArA==) with multiple visit dates, whether BP calculations were correct:
     # sbp_clean1 %>% filter(PATIENT_LINKAGE == "+++3bBI01w/HnR669dgArA==") %>% 
     #   select(PATIENT_LINKAGE, MEASUREMENT_DATE,SBP)
     # 
     # dbp_clean1 %>% filter(PATIENT_LINKAGE == "+++3bBI01w/HnR669dgArA==") %>% 
     #   select(PATIENT_LINKAGE, MEASUREMENT_DATE,DBP)
     # 
     # all_pop_clean2 %>% filter(PATIENT_LINKAGE == "+++3bBI01w/HnR669dgArA==") %>%
     #   select(PATIENT_LINKAGE, MEASUREMENT_DATE, SBP_avg, DBP_avg)
     # 
     # all_pop_clean3 %>% filter(PATIENT_LINKAGE == "+++3bBI01w/HnR669dgArA==") %>%
     #   select(PATIENT_LINKAGE, MEASUREMENT_DATE, SBP_avg, DBP_avg)

     
     
   
     
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#  
   
# Step 5. High BP identification (140/90 and 130/80 thresholds) -----------
   
# First, select only the measurement year 
   str(all_pop_clean3$MEASUREMENT_DATE)
   all_pop_clean3$MEASUREMENT_DATE <- as.Date(all_pop_clean3$MEASUREMENT_DATE)
   all_pop_clean3$m.year <- as.numeric(format(all_pop_clean3$MEASUREMENT_DATE,'%Y')) # base R much faster than tidyverse
   
   # for high BP calculation, use 2022 as measurement year, drop 2021  
   all_pop_clean3_2023 <- all_pop_clean3 %>% 
     filter(m.year == 2023) 
   
   export(all_pop_clean3_2023, here("data","clean","all_pop_clean3_2023.csv"))
   
   
   
## 5.a. BP >= 140/90 mmHg ----
   
  ## create high BP variable #1
   
  # Must faster codes using functions from data.table package instead of dplyr:

  # group by each patient and each visit date

     high140a <- all_pop_clean3_2023 %>% group_by(PATIENT_LINKAGE, MEASUREMENT_DATE)
       high140a$hbp140 <- ifelse((high140a$SBP_avg >=140 | high140a$DBP_avg >=90), 1,0)  # create high BP variable at 140/90 cutoff for each visit
       high140a <- high140a %>% ungroup()                           
   
     # check to see if the high BP variable is correctly created:
       print(high140a, n=100)  
       # can test this patient later: +++4GHti6arD9MyKz4WEnA== 
     
  
  # must have high BP on two or more separate days for this hypertension e-phenotype

   high140b <- high140a %>%
     group_by(PATIENT_LINKAGE) %>%
     summarise(n = sum(hbp140, na.rm = T)) # will not keep piping because will be slow, revert to base R:
   
   high140b$HTN140_90 <- ifelse(high140b$n>1, 1, 0)  # HTN140 is coded as 1, if a patient has high BP on 2 or more days
   
  # mark high BP back in the main dataset at the 140/90 threshold
   all_pop_merged2 <- all_pop_clean4 %>% 
     left_join(high140b, by = "PATIENT_LINKAGE") %>% 
     select(-n)
  
   # check to see if the high BP variable is correctly created:
     print(all_pop_merged2, n=20)  
     # can test this patient later: +++4GHti6arD9MyKz4WEnA== (should be "1" for HTN_140_90)
   
   

## 5.b. BP >= 130/80 mmHg ----
   
   # Must faster codes using functions from data.table package:

   # group by each patient and each visit date
     
   high130a <- all_pop_clean3_2023 %>% group_by(PATIENT_LINKAGE, MEASUREMENT_DATE)
    high130a$hbp130 <- ifelse((high140a$SBP_avg >=130 | high140a$DBP_avg >=80), 1,0)  # create high BP variable at 140/90 cutoff for each visit
    high130a <- high130a %>% ungroup()              
   
   # check to see if the high BP variable is correctly created:
     print(high130a, n=100)  
     # can test this patient later: +++4GHti6arD9MyKz4WEnA== 
   
   
   # must have high BP on two or more separate days for this hypertension e-phenotype
   
   high130b <- high130a %>%
     group_by(PATIENT_LINKAGE) %>%
     summarise(n = sum(hbp130, na.rm = T)) # will not keep piping because will be slow, revert to base R:
   
   high130b$HTN130_80 <- ifelse(high130b$n>1, 1, 0)  # HTN140 is coded as 1, if a patient has high BP on 2 or more days
   
   # mark high BP back in the main dataset at the 140/90 threshold
   all_pop_merged3 <- all_pop_merged2 %>% 
     left_join(high130b, by = "PATIENT_LINKAGE") %>% 
     select(-n)
   
   
   # check to see if the high BP variable is correctly created:
   
   ## can test this patient: ++1y9CBp8UcIDMqdDMcDYw== (should be "1" for HTN_130_80, but "0" for HTN_140_90)
   #   both_bp %>% filter(PATIENT_LINKAGE ==  "++1y9CBp8UcIDMqdDMcDYw==")
   #   print(all_pop_merged3, n=100)  
     
   # export this intermediate subset:  
   export(all_pop_merged3, here("data","clean","all_pop_merged3.csv"))
   
   

   
   
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#  
   
# Step 6. Hypertension control -----------
   
## 6.a. Most recent BP in the measurement year was <140/90----
   
   BP_control140a <- all_pop_clean3_2023 %>% 
     group_by(PATIENT_LINKAGE) %>%
     slice(which.max(as.Date(MEASUREMENT_DATE, '%Y/%m/%d'))) %>% 
     select(-m.year)
   
   BP_control140a <- BP_control140a %>% ungroup()
   
# check to see if only the most recent date was kept during the measurement year:
     head(all_pop_clean3_2023, 10)
     head(BP_control140a, 10)
     
# create dummy variable for BP control at 140/90 threshold
   BP_control140a$HTNcontrol140 <- ifelse(BP_control140a$SBP_avg < 140 & BP_control140a$DBP_avg <90, 1, 0)
     
# merged this new BP control variable back to the main dataset:  
   all_pop_merged4 <- all_pop_merged3 %>% 
     left_join(BP_control140a, by = "PATIENT_LINKAGE") %>% 
     select(-MEASUREMENT_DATE, -SBP_avg, -DBP_avg) %>% 
     distinct(PATIENT_LINKAGE, .keep_all = TRUE)
   
   head(all_pop_merged4, 10)
   
   
   
   
## 6.b. Most recent BP in the measurement year was <130/80 ----
   BP_control130a <- all_pop_clean3_2023 %>% 
     group_by(PATIENT_LINKAGE) %>%
     slice(which.max(as.Date(MEASUREMENT_DATE, '%Y/%m/%d'))) %>% 
     select(-m.year)
   
   BP_control130a <- BP_control130a %>% ungroup()
   
 # check to see if only the most recent date was kept during the measurement year:
   head(all_pop_clean3_2023, 10)
   head(BP_control130a, 10)
   
 # create dummy variable for BP control at 140/90 threshold
   BP_control130a$HTNcontrol130 <- ifelse(BP_control130a$SBP_avg < 130 & BP_control130a$DBP_avg <80, 1, 0)
 
   
# merged this new BP control variable back to the main dataset:  
   all_pop_merged5 <- all_pop_merged4 %>% 
     left_join(BP_control130a, by = "PATIENT_LINKAGE") %>% 
     distinct(PATIENT_LINKAGE, .keep_all = TRUE) 

   # quality check:
   head(all_pop_clean3_2023, 10)
   head(all_pop_merged5, 10)
   
   export(all_pop_merged5, here("data","clean","all_pop_merged5.csv"))
   
 
   
## use this analytical subset for all subsequent analyses:
   
   analytical <- all_pop_merged5 %>% 
     select(PATIENT_LINKAGE, age, sex, race, state, zip3, 
            HTN140_90, HTN130_80, HTNcontrol140, HTNcontrol130)
   

   export(analytical, here("data","clean","analytical.csv"))
   
   
   

   
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#
# Archived codes -----------------------------------------------------

## VERY slow code below. Note to analysts: feel free to help check why these don't go through properly. 
   # the codes below work on smaller subset, but failed to converge for full dataset
   # Tidyverse suite seems to be much slower than base R when using alrge datasets

   
## Create high BP variable: (run time > 4 hours, did not finish)
   
   # highBP140a <- all_pop_clean3_2023 %>% 
   #   group_by(PATIENT_LINKAGE, MEASUREMENT_DATE) %>% 
   #   
   #    mutate(hbp140 = case_when(
   #     SBP_avg >= 140 | DBP_avg >=90 ~ 1,
   #     SBP_avg < 140 & DBP_avg < 90  ~ 0
   #     #TRUE                  ~ NA)  # remove this to see if it's faster; nope still slow
   #     )) 
   # 
   # highBP140b <- highBP140a %>% ungroup()
   #   
   
   
   
## Create BP control variable:
   
   # mutate(control140 = case_when(
   #   SBP_avg < 140 & DBP_avg <90 ~ 1,
   #   SBP_avg >=140 & DBP_avg>=90 ~ 0)
   # ) %>% 
   # 
   # select(PATIENT_LINKAGE, control140)
   
   
## data.table's group function stopped working (the line below)
   # high140a <- all_pop_clean3_2023[, by = .("PATIENT_LINKAGE", "MEASUREMENT_DATE")] 
   