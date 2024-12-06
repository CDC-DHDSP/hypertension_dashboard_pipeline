### PART 2 #################################################

###>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>###
###  Project: AEMR-US OMOP adult Hypertension Phenotype  ###
###                                                      ###
###      PART 2: Data importing & cleaning               ###
###   Remove ineligible patients (preg, esrd, care)      ###
###                                                      ###
###           Data source: IQVIA AEMR-US OMOP            ###
###   Script Author: Siran He, CDC |  Jul-Aug 2023       ###
###  Modified for DSU Team BP Commanders | Dec 2024      ### 
###                                                      ###
###>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>###



# Step 1. Install useful packages ----------------------------------------------------------------

  # library(pacman)
  pacman::p_load(
    DBI,
    rio,
    here,
    odbc, # helps connect to a SQL server & query data, has tools to explore objects & columns in the database
    sqldf, # manipulate R data frames using SQL
    dbconnect,
    glue,
    dbplyr, # using dplyr functions through R & SQL combined
    skimr, # browse a dataset
    janitor,
    tidyverse
    #curl,
    #httr,
    #lubridate,
    #stringr,
    #gtsummary
  )

# as needed, start from the middle (line 160) to save time --
  #  all_pop3c  <- read.csv("C:\\Users\\tdu4\\weighting_project\\data\\clean\\all_pop3c.csv")



# Step 2. Initial data cleaning -------------------------------------------

# Load the database name ---
  
  #' [This is the actual full dataset with 12M + patients]
  #  db_name<-("AEMR_2023MAIN_WITH2022_06SEP2024_202737_699.DBO")
  
  #' [For practice purpose, use this mini dataset with 10.000 patients]
  db_name<-("SAMPLE_AEMR_2023MAIN_WITH2022_02DEC2024_202010_937.DBO")
  
# Set strings for measurement periods ---
  year_string_1<-("2023")  # main study period
  year_string_2<-("2022, 2023") # study period + one year look-back for BP check
  

## 2.a. Starting population (all adults age 18+ in the measurement year) ----

  demo_query <-(
    gsub("\n", "", 
         paste0(
           "SELECT 
                PATIENT_LINKAGE
               ,YEAR_OF_BIRTH
               ,TRIM (ETHNICITY_SOURCE_VALUE,'\"') AS RACE
               ,TRIM (GENDER_SOURCE_VALUE,'\"') AS SEX
               ,TRIM (LOCATION_ZIP,'\"') AS ZIP3
               ,LOCATION_STATE AS STATE 
          FROM
          ",db_name,".PERSON
          "
         )))
  
  all_pop <- dbGetQuery(con, demo_query)
  str(all_pop)
  skim(all_pop) # Check: missing patient_linkage (n=6 number of patients)
  
  all_pop2 <- all_pop %>% 
    filter(!is.na(PATIENT_LINKAGE)) # Check: n = 9,994 has PATIENT_LINKAGE
  
  

## 2.b. Investigate duplicates & "mis-bridged" records for selected variables ----

  # See the word file about duplicate_rules in "output" folder of this R project
  # "mis-bridge" refers to the same patient_linkage but with multiple ages, sexes, race, etc.
  
   skim(all_pop2)
   
  # Year of birth
     all_pop2 %>% 
       group_by(PATIENT_LINKAGE) %>% 
       summarise(yob_count = n_distinct(YEAR_OF_BIRTH)) %>% 
       tabyl(yob_count)
     
  # Sex   
     all_pop2 %>% 
       group_by(PATIENT_LINKAGE) %>% 
       summarise(sex_count = n_distinct(SEX)) %>% 
       tabyl(sex_count)
     
  # Race   
     all_pop2 %>% 
       group_by(PATIENT_LINKAGE) %>% 
       summarise(race_count = n_distinct(RACE)) %>% 
       tabyl(race_count)
     
  # Ethnicity: Please note that IQVIA's ethnicity field is exactly the same as race
       # all_pop2 %>% 
       #   group_by(PATIENT_LINKAGE) %>% 
       #   summarise(ethn_count = n_distinct(ETHNICITY)) %>% 
       #   tabyl(ethn_count)  
   
  # State  
     all_pop2 %>% 
       group_by(PATIENT_LINKAGE) %>% 
       summarise(state_count = n_distinct(STATE)) %>% 
       tabyl(state_count)
   
  # Zip3  
     all_pop2 %>% 
       group_by(PATIENT_LINKAGE) %>% 
       summarise(zip3_count = n_distinct(ZIP3)) %>% 
       tabyl(zip3_count)
     
     
     
## 2.c. Clean up duplicates & handle mis-bridged records----
     
  # For year of birth, sex, race/ethnicity: keep only patient_linkage with unique values for these 3 variables
  
     all_pop3 <- all_pop2 %>% 
       group_by(PATIENT_LINKAGE) %>% 
       summarise(yob_count = n_distinct(YEAR_OF_BIRTH),
                 sex_count = n_distinct(SEX),
                 race_count = n_distinct(RACE)) %>% 
       filter(yob_count == 1 & sex_count == 1 & race_count == 1)
     
     all_pop3 <- all_pop3 %>% 
       left_join(all_pop2, by = "PATIENT_LINKAGE")
   
  
  # For state & zip3: remove missing ones, and randomly keep one out of duplicated state & zip3 
   
     all_pop3a <- all_pop3 %>% 
       filter(!is.na(STATE) & !is.na(ZIP3))
     
     all_pop3b <- all_pop3a %>% 
       group_by(PATIENT_LINKAGE, STATE) %>% 
       distinct(.keep_all = TRUE)
     
     all_pop3c <- all_pop3b %>% 
       group_by(PATIENT_LINKAGE, ZIP3) %>% 
       distinct(.keep_all = TRUE)
   
  
 # Double check duplication: 
     
    check <- distinct(all_pop3c)  # 11303190, no more dupes
    
 # [optional] free up some space
    remove(check, all_pop3, all_pop3a, all_pop3b) 
    

 # Exported this intermediate file for later use, because the steps to create all_pop3 is slow
    
    export(all_pop3c, here("data","clean","all_pop3c.csv"))
    
    # to use:
      # very slow: all_pop3c <- import(here("data","clean","all_pop3c.csv"))
      # much faster: all_pop3c <- read.csv("C:\\Users\\tdu4\\IQVIA_SH\\data\\clean\\all_pop3c.csv")
    
    
    
## 2.d Age restrictions ---- 
   
## Notes about age cap/truncation:
    # For patient privacy protection, age is capped at 85y in data access year (in this case, 2023)
    # Back-calculated this age in the measurement year: for example in 2022, cap age would be 84y
    # Thus need to trim to 83y as max age in 2022, because we will not be sure if 84y patients are actually 84y or older
    # Analyst could further trim the age range as needed 

# 18+ in 2023:
  all_adults <- all_pop3c %>%
    filter(YEAR_OF_BIRTH <=2005)
    
  
## 2.e Subset: women of reproductive age (WRA), Female, aged 18-44y ----
  
 # str(all_pop3c$SEX) # "\"M\""  "\"F\"" 
    
 # WRA aged 18-44 in 2023
   WRA <- all_pop3c %>%
     filter((YEAR_OF_BIRTH >=1979 & YEAR_OF_BIRTH <=2005) &
             SEX == "F"
     )

    

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#  

# Step 3. Import code lists for each exclusion criteria ----------------------------

## 3.a. Pregnancy codes ----

   preg_condition <- import(here("data","import","preg_condition_standard_codelist.xlsx"))
   preg_condition_codelist <- paste(preg_condition[[1]], collapse = ",")
     # 914 codes
   
   preg_measure <- import(here("data","import","preg_measurement_standard_codelist.xlsx"))
   preg_measure_codelist <- paste(preg_measure[[1]], collapse = ",")
     # 2 codes 
   
   preg_observe <- import(here("data","import","preg_observation_standard_codelist.xlsx"))
   preg_observe_codelist <- paste(preg_observe[[1]], collapse = ",")
     # 35 codes 
   
   preg_procedure <- import(here("data","import","preg_procedure_standard_codelist.xlsx"))
   preg_procedure_codelist <- paste(preg_procedure[[1]], collapse = ",")
     # 1 code

   
## 3.b. End-stage renal disease (ESRD) codes ----
   
   esrd_condition <- import(here("data","import","esrd_condition_standard_codelist.xlsx"))
   esrd_condition_codelist <- paste(esrd_condition[[1]], collapse = ",")
     # 2 codes
   
   esrd_observe <- import(here("data","import","esrd_observation_standard_codelist.xlsx"))
   esrd_observe_codelist <- paste(esrd_observe[[1]], collapse = ",")
     # 29 codes; used only codes for ages: "all", "12-19" and "over 20"; did not use "<2" or "2-11"
   
   esrd_procedure <- import(here("data","import","esrd_procedure_standard_codelist.xlsx"))
   esrd_procedure_codelist <- paste(esrd_procedure[[1]], collapse = ",")
     # 50 codes; used only codes for ages: "all", "12-19" and "over 20"; did not use "<2" or "2-11"
   
   
#' [Note to BP Commanders: long-term care codes has been retrieving zero patients in the past few data releases, so I hid section 3c for now]   
## 3.c. Long-term care codes ----
   
   # ltc_procedure <- import(here("data","import","ltc_procedure_standard_codelist.xlsx"))
   # ltc_procedure_codelist <- paste(ltc_procedure[[1]], collapse = ",")
   #   # 19 codes
   # 
   # ltc_observe <- import(here("data","import","ltc_observation_standard_codelist.xlsx"))
   # ltc_observe_codelist <- paste(ltc_observe[[1]], collapse = ",")
   #    # 4 codes
   

## 3.d. Palliative care codes ----
   palliative_procedure <- import(here("data","import","palliative_procedure_standard_codelist.xlsx"))
   palliative_procedure_codelist <- paste(palliative_procedure[[1]], collapse = ",")
      # 2 codes
   
   palliative_observe <- import(here("data","import","palliative_observation_standard_codelist.xlsx"))
   palliative_observe_codelist <- paste(palliative_observe[[1]], collapse = ",")
      # 17 codes

   
## 3.e. Hospice care codes ----
   hospice_procedure <- import(here("data","import","hospice_procedure_standard_codelist.xlsx"))
   hospice_procedure_codelist <- paste(hospice_procedure[[1]], collapse = ",")
     # 1 code
   
   hospice_observe <- import(here("data","import","hospice_observation_standard_codelist.xlsx"))
   hospice_observe_codelist <- paste(hospice_observe[[1]], collapse = ",")
     # 4 codes 
   
   
   
   
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#  
   
# Step 4. Exclude ineligible patients -------------------------------------

## 4.a. Exclude pregnancy among women aged 18-44y in the measurement year ----
   
### the following scripts identified patients with pregnancy-related codes in the 4 domains, then merged them together for exclusion
  
 # condition domain
   preg_c_query <-
     gsub("\n", "", 
          paste(
            paste0("SELECT 
                    P.PATIENT_LINKAGE
                    ,'1' AS PREG_CONDITION 
                    
                  FROM      ",db_name,".PERSON AS P
                  LEFT JOIN ",db_name,".CONDITION_OCCURRENCE AS C
                  ON P.PATIENT_LINKAGE = C.PATIENT_LINKAGE
                
                  WHERE 
                     P.GENDER_CONCEPT_ID = 8532
                  AND
                     P.YEAR_OF_BIRTH BETWEEN ",year_string_1,"-44 AND ",year_string_1,"-18
                  AND
                     C.CONDITION_CONCEPT_ID IN (",preg_condition_codelist,") 
                  AND
                     YEAR(C.CONDITION_START_DATE) IN (",year_string_1,")
                  "
            )))
   
   exc_preg_c <- dbGetQuery(con, preg_c_query) 

   
 # measurement domain
   preg_m_query <-
     gsub("\n", "", 
          paste(
            paste0("SELECT 
                    P.PATIENT_LINKAGE
                    ,'1' AS PREG_MEASUREMENT
                    
                  FROM      ",db_name,".PERSON AS P
                  LEFT JOIN ",db_name,".MEASUREMENT AS M
                  ON P.PATIENT_LINKAGE = M.PATIENT_LINKAGE
                
                  WHERE 
                     P.GENDER_CONCEPT_ID = 8532
                  AND
                     P.YEAR_OF_BIRTH BETWEEN ",year_string_1,"-44 AND ",year_string_1,"-18
                  AND
                     M.MEASUREMENT_CONCEPT_ID IN (",preg_measure_codelist,") 
                  AND
                     YEAR(M.MEASUREMENT_DATE) IN (",year_string_1,")
                  "
            )))
   
   exc_preg_m <- dbGetQuery(con, preg_m_query) 
   
   
   
 # observation domain
   preg_o_query <-
     gsub("\n", "", 
          paste(
            paste0("SELECT 
                    P.PATIENT_LINKAGE
                    ,'1' AS PREG_OBSERVATION
                    
                  FROM      ",db_name,".PERSON AS P
                  LEFT JOIN ",db_name,".OBSERVATION AS O
                  ON P.PATIENT_LINKAGE = O.PATIENT_LINKAGE
                
                  WHERE 
                     P.GENDER_CONCEPT_ID = 8532
                  AND
                     P.YEAR_OF_BIRTH BETWEEN ",year_string_1,"-44 AND ",year_string_1,"-18
                  AND
                     O.OBSERVATION_CONCEPT_ID IN (",preg_observe_codelist,") 
                  AND
                     YEAR(O.OBSERVATION_DATE) IN (",year_string_1,")
                  "
            )))
   
   exc_preg_o <- dbGetQuery(con, preg_o_query) 
   
  
 # procedure domain
   
   preg_pr_query <-
     gsub("\n", "", 
          paste(
            paste0("SELECT 
                    P.PATIENT_LINKAGE
                    ,'1' AS PREG_PROCEDURE
                    
                  FROM      ",db_name,".PERSON AS P
                  LEFT JOIN ",db_name,".PROCEDURE_OCCURRENCE AS PR
                  ON P.PATIENT_LINKAGE = PR.PATIENT_LINKAGE
                
                  WHERE 
                     P.GENDER_CONCEPT_ID = 8532
                  AND
                     P.YEAR_OF_BIRTH BETWEEN ",year_string_1,"-44 AND ",year_string_1,"-18
                  AND
                     O.PROCEDURE_CONCEPT_ID IN (",preg_procedure_codelist,") 
                  AND
                     YEAR(O.PROCEDURE_DATE) IN (",year_string_1,")
                  "
            )))
   
   exc_preg_pr <- dbGetQuery(con, preg_pr_query) 
   
   
  # Full join all domains
   exc_preg <- exc_preg_c %>% 
     full_join(exc_preg_m, by = "PATIENT_LINKAGE") %>% 
     full_join(exc_preg_o, by = "PATIENT_LINKAGE") %>% 
     full_join(exc_preg_pr, by = "PATIENT_LINKAGE")

  # Exclude pregnancy from women of reproductive age subset; incorporate back to main dataset
   all_pop_ex <- WRA %>% 
     inner_join(exc_preg, by = "PATIENT_LINKAGE") %>% 
     distinct(.keep_all = TRUE)
   
   all_pop_ex1 <- all_pop3c %>% 
     anti_join(exc_preg, by = "PATIENT_LINKAGE") 

  ## Calculate percentage of excluded pregnancy in relation to WRA population:
       percent1 <- 100*(nrow(all_pop3c) - nrow(all_pop_ex1))/ nrow(WRA) 
       print(percent1)  
       # 8.97% of WRA excluded due to pregnancy -- slightly less than paper 1 (9.26%)
       
   


## 4.b. Exclude ESRD among adult patients in the measurement year ----
   
### the following scripts identified patients with ESRD codes in the 3 domains, then merged them together for exclusion
   
 # condition domain 
   esrd_c_query <-
     gsub("\n", "", 
          paste(
            paste0("SELECT 
                PATIENT_LINKAGE
                ,'1' AS ESRD_CONDITION 
              FROM 
                 ",db_name,".CONDITION_OCCURRENCE 
              WHERE 
                 CONDITION_CONCEPT_ID IN (",esrd_condition_codelist,") 
              AND
                 YEAR(C.CONDITION_START_DATE) IN (",year_string_1,")
              "
            )))
   
   exc_esrd_c <- dbGetQuery(con, esrd_c_query) 

   
 # observation domain  
   esrd_o_query <-
     gsub("\n", "", 
          paste(
            paste0("SELECT 
                PATIENT_LINKAGE
                ,'1' AS ESRD_OBSERVATION 
              FROM 
                 ",db_name,".OBSERVATION 
              WHERE 
                 OBSERVATION_CONCEPT_ID IN (",esrd_observe_codelist,") 
              AND
                 YEAR(OBSERVATION_DATE) IN (",year_string_1,")
              "
            )))
   
   exc_esrd_o <- dbGetQuery(con, esrd_o_query) 
   
 
 # procedure domain 
   esrd_pr_query <-
     gsub("\n", "", 
          paste(
            paste0("SELECT 
                PATIENT_LINKAGE
                ,'1' AS ESRD_PROCEDURE
              FROM 
                 ",db_name,".PROCEDURE_OCCURRENCE 
              WHERE 
                 PROCEDURE_CONCEPT_ID IN (",esrd_procedure_codelist,") 
              AND
                 YEAR(PROCEDURE_DATE) IN (",year_string_1,")
              "
            )))
   
   exc_esrd_pr <- dbGetQuery(con, esrd_pr_query) 
  
   
 # Full join all domains  
   exc_esrd <- exc_esrd_c %>% 
     full_join(exc_esrd_o, by = "PATIENT_LINKAGE") %>% 
     full_join(exc_esrd_pr, by = "PATIENT_LINKAGE")

 # Exclude from main dataset  
   all_pop_ex2 <- all_pop_ex1 %>% 
     anti_join(exc_esrd, by = "PATIENT_LINKAGE")
     # n = 12,842,331
   
   
 ## Calculate percentage of excluded ESRD in relation to total population:
   percent2 <- 100*(nrow(all_pop_ex1) - nrow(all_pop_ex2))/ nrow(all_pop3c) 
   print(percent2)   # 0.18% of patients excluded due to ESRD -- similar to paper 1 (0.19%)
 
   

## 4.c. Exclude patients in care: LTC, palliative, or hospice care in the measurement year ----
   
#' [removed long-term care section because zero patients were retrieved] 
   ### i. Exclude patients in long-term care among adults aged > 65y in the measurement year 
   # 
   # exc_ltc_o <- dbGetQuery(con, paste(
   # "SELECT 
   #    p.patient_linkage
   #    ,'1' as ltc_observation
   # FROM AEMR_2023MAIN_WITH2022_06SEP2024_202737_699.DBO.PERSON AS p
   #    LEFT JOIN AEMR_2023MAIN_WITH2022_06SEP2024_202737_699.DBO.observation AS o
   #    ON p.patient_linkage = o.patient_linkage
   # WHERE p.year_of_birth < 1957
   # AND o.OBSERVATION_CONCEPT_ID IN (",ltc_observe_codelist,")  
   # AND YEAR(o.OBSERVATION_DATE) IN (2023) 
   # ", 
   #   sep=""))
   # # n = 0 
   # 
   # 
   # exc_ltc_pr <- dbGetQuery(con, paste(
   # "SELECT 
   #    p.patient_linkage
   #    ,'1' as ltc_procedure
   # FROM AEMR_2023MAIN_WITH2022_06SEP2024_202737_699.DBO.PERSON AS p
   #    LEFT JOIN AEMR_2023MAIN_WITH2022_06SEP2024_202737_699.DBO.procedure_occurrence AS pr
   #    ON p.patient_linkage = pr.patient_linkage
   # WHERE p.year_of_birth < 1957
   # AND pr.PROCEDURE_CONCEPT_ID IN (",ltc_procedure_codelist,") 
   # AND YEAR(pr.PROCEDURE_DATE) IN (2023) 
   # ", 
   #   sep=""))
   # # n = 0 
   
   # Similar to what was in cohort builder, there were zero patients aged 66+ in LTC 
  
   
   
  ### ii. Exclude patients in palliative care among adult patients in the measurement year (2 domains: observation, procedure)
   
   # observation domain
   palli_o_query <-
     gsub("\n", "", 
          paste(
            paste0("SELECT 
                PATIENT_LINKAGE
                ,'1' AS PALLI_OBSERVATION 
              FROM 
                 ",db_name,".OBSERVATION 
              WHERE 
                 OBSERVATION_CONCEPT_ID IN (",palliative_observe_codelist,") 
              AND
                 YEAR(OBSERVATION_DATE) IN (",year_string_1,")
              "
            )))
   
   exc_palliative_o <- dbGetQuery(con, palli_o_query) 
   
  
   # procedure domain
   palli_pr_query <-
     gsub("\n", "", 
          paste(
            paste0("SELECT 
                PATIENT_LINKAGE
                ,'1' AS PALLI_PROCEDURE 
              FROM 
                 ",db_name,".PROCEDURE_OCCURRENCE 
              WHERE 
                 PROCEDURE_CONCEPT_ID IN (",palliative_procedure_codelist,") 
              AND
                 YEAR(PROCEDURE_DATE) IN (",year_string_1,")
              "
            )))
   
   exc_palliative_pr <- dbGetQuery(con, palli_pr_query) 
   
 
   ### iii. Exclude patients in hospice care among adult patients in the measurement year (2 domains: observation, procedure)
   
   # observation domain
   hos_o_query <-
     gsub("\n", "", 
          paste(
            paste0("SELECT 
                PATIENT_LINKAGE
                ,'1' AS HOS_OBSERVATION 
              FROM 
                 ",db_name,".OBSERVATION 
              WHERE 
                 OBSERVATION_CONCEPT_ID IN (",hospice_observe_codelist,") 
              AND
                 YEAR(OBSERVATION_DATE) IN (",year_string_1,")
              "
            )))
   
   exc_hos_o <- dbGetQuery(con, hos_o_query) 
   
  
   # procedure domain
   hos_pr_query <-
     gsub("\n", "", 
          paste(
            paste0("SELECT 
                PATIENT_LINKAGE
                ,'1' AS HOS_PROCEDURE 
              FROM 
                 ",db_name,".PROCEDURE_OCCURRENCE 
              WHERE 
                 PROCEDURE_CONCEPT_ID IN (",hospice_procedure_codelist,") 
              AND
                 YEAR(PROCEDURE_DATE) IN (",year_string_1,")
              "
            )))
   
   exc_hos_pr <- dbGetQuery(con, hos_pr_query) 
   


  # Merge all domains 
   exc_care <- exc_palliative_o %>% 
     full_join(exc_palliative_pr, by = "PATIENT_LINKAGE") %>% 
     full_join(exc_hospice_o, by = "PATIENT_LINKAGE") %>% 
     full_join(exc_hospice_pr, by = "PATIENT_LINKAGE")

  # Exclude from main dataset 
   all_pop_ex3 <- all_pop_ex2 %>% 
     anti_join(exc_care, by = "PATIENT_LINKAGE")


 ## Calculate percentage of excluded patients in various care in relation to total population:
   percent3 <- 100*(nrow(all_pop_ex2) - nrow(all_pop_ex3))/ nrow(all_pop3c) 
   print(percent3)   # 0.01% of patients excluded due to in care -- less than to paper 1 (0.19%)
   
 
   
   
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#  

# Step 5. Export intermediate dataset for subsequent analyses -------------

  ## Browse the current labels:
     # tabyl(all_pop_ex3$SEX)
     # tabyl(all_pop_ex3$RACE)
     # tabyl(all_pop_ex3$ETHNICITY)
     # tabyl(all_pop_ex3$STATE)
     # tabyl(all_pop_ex3$ZIP3)
   
   
  ## Assign cleaner labels for subsequent analyses:
   
   all_pop_clean_a <-all_pop_ex3 %>% 
     select(-yob_count, -sex_count, -race_count, -ETHNICITY) 
   
   all_pop_clean_a$state <-  all_pop_clean_a$STATE
   all_pop_clean_a$age <- 2023 - all_pop_clean_a$YEAR_OF_BIRTH
   all_pop_clean_a$race <- recode(all_pop_clean_a$RACE,
                                  "AFRICAN AMERICAN" = "Black",
                                  "ASIAN" = "Asian",
                                  "CAUCASIAN" = "White",
                                  "HISPANIC" = "Hispanic",
                                  "OTHER" = "Other",
                                  "UNKNOWN" = "Unknown")
   all_pop_clean_a$sex <- recode(all_pop_clean_a$SEX,
                                 "F" = "Female",
                                 "M" = "Male")
   
   all_pop_clean_a$zip3 <- str_sub(all_pop_clean_a$ZIP3, 2, -2)
   
   all_pop_clean <- all_pop_clean_a %>% 
     select(-RACE, -SEX, -STATE, -ZIP3)
   
   
## export:
   export(all_pop_clean, here("data","clean","all_pop_clean.csv"))
   
   
   
   
   
   
   
## [ARCHIVE] DO NOT RUN. These are just example / test codes based on Ran's previous work ------------------------------
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#  

#' [***This section is just for testing, actual step starts from Step 2 above***] 
#' [Ran's example on how to pre-assign database name and study year, to avoid having to change each relevant line of code throughout]

# Load the database name ---

#' [This is the actual full dataset with 12M + patients]
#  db_name<-("AEMR_2023MAIN_WITH2022_06SEP2024_202737_699.DBO")

#' [For practice purpose, use this mini dataset with 1000 patients]
db_name<-("RANDOM1000_AEMR_2023MAIN_WITH2022_02DEC2024_201506_513.DBO")

# Set strings for measurement periods ---
year_string_1<-("2023")  # main study period
year_string_2<-("2022, 2023") # study period + one year lookback for BP check


# Query data using the strings above (testing retriving hypertension patients with diagnosis codes)

# Load the excel sheets in vectors ---
code_list<-purrr::flatten(import_list(here("data/import/code_lists.xlsx")))
dx_list<-paste(code_list$dx_code, collapse = ",") # convert to string

htn_dx_query <-
 gsub("\n", "", 
      paste(
        paste0("SELECT 
                 PATIENT_LINKAGE
                 ,CONDITION_CONCEPT_ID AS CONCEPT_ID
                 ,TRIM (CONDITION_CONCEPT_DESC,'\"') AS CONDITION_DESCRIPTION
                 ,CONDITION_START_DATE
                 ,'1' AS HTN_DX 
              FROM 
                 ",db_name,".CONDITION_OCCURRENCE
              WHERE 
                 CONDITION_CONCEPT_ID IN (",dx_list,") 
              AND
                 YEAR(CONDITION_START_DATE) IN (",year_string_1,")
                 
              LIMIT 20
              "
        )))

df_htn_dx <- dbGetQuery(con, htn_dx_query) 
str(df_htn_dx)


# Test query for demographic variables
demo_query <-(
 gsub("\n", "", 
      paste0(
        "SELECT 
            PATIENT_LINKAGE
           ,YEAR_OF_BIRTH
           ,TRIM (ETHNICITY_SOURCE_VALUE,'\"') AS RACE
           ,TRIM (GENDER_SOURCE_VALUE,'\"') AS SEX
           ,TRIM (LOCATION_ZIP,'\"') AS ZIP3
           ,LOCATION_STATE AS STATE 
FROM
",db_name,".PERSON
LIMIT 20
"
      )))

df_demo <- dbGetQuery(con, demo_query)
head(df_demo)

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#  
   
   
 