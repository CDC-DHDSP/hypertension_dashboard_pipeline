### PART 4 #################################################

###>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>###
###  Project: AEMR-US OMOP adult Hypertension Phenotype  ###
###                                                      ###
###         PART 4: Hypertension e-Phenotype             ###
###                     Dx, BP, Meds                     ###
###                                                      ###
###           Data source: IQVIA AEMR-US OMOP            ###
###   Script Author: Siran He, CDC |  Aug-Sep 2023       ###
###         Updated: Kara Beck, ORISE | Oct 2023         ###
###        Certain parts adapted from XW script          ###
###  Modified for DSU Team BP Commanders | Dec 2024      ### 
###                                                      ###
###>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>###



# Step 1. Install useful packages ----------------------------------------------------------------

pacman::p_load(
  DBI, #ODBC connection package
  skimr,
  rio,
  here,
  dplyr,
  janitor,
  lubridate,
  tidyverse
  #stringr,
  #gtsummary
)


# Step 2. Import data from the previous part -----------------------------------------------------

  # analytical <- read.csv("C:\\Users\\tdu4\\weighting_project\\data\\clean\\htn_dx_patients.csv")
  # htn_dx_patients <- read.csv("C:\\Users\\tdu4\\weighting_project\\data\\clean\\htn_dx_patients.csv")
  # htn_meds_patients <- read.csv("C:\\Users\\tdu4\\weighting_project\\data\\clean\\htn_meds_patients.csv")

## for portable Rproject analysis: 
   #analytical <- import(here("data","clean","analytical.csv"))
   analytical <- import(here("data","clean","analytical.csv"))
   
   
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#
  
# Step 3. Import Dx and Meds code lists -----------------------------------

  code_list<-purrr::flatten(import_list(here("data/import/code_lists.xlsx")))
  # HTN diagnosis codes
  dx_list<-paste(code_list$dx_code, collapse = ",")
  # Antihypertensive medication codes
  rx_list<-paste(code_list$rx_code, collapse = ",")

  
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#
  
# Step 4. Dx: Hypertension diagnosis codes in the measurement period --------------------------------

# Query patients with Dx: 
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
              
                  "
           )))
  
  htn_dx_patients <- dbGetQuery(con, htn_dx_query) 
  str(htn_dx_patients)
  
  
  
  
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#
  
# Step 5. Meds: Antihypertensive medications in the measurement period---------------------------------------------------
  
  # Query patients with Rx: 
  htn_rx_query <-
    gsub("\n", "", 
         paste(
           paste0("SELECT 
                     PATIENT_LINKAGE
                     ,DRUG_CONCEPT_ID
                     ,DRUG_EXPOSURE_START_DATE
                     ,'1' AS MEDS
                  FROM 
                     ",db_name,".DRUG_EXPOSURE
                  WHERE 
                     DRUG_CONCEPT_ID IN (",rx_list,") 
                  AND
                     YEAR(DRUG_EXPOSURE_START_DATE) IN (",year_string_1,")
              
                  "
           )))
  
  htn_meds_patients <- dbGetQuery(con, htn_rx_query) 
  str(htn_meds_patients)
  
 
  # Export these two datasets because the steps above are a bit slow:
    export(htn_dx_patients, here("data","clean","htn_dx_patients.csv"))
    export(htn_meds_patients, here("data","clean","htn_meds_patients.csv"))
 
    
     
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#
    
# Step 6. Apply the hypertension e-phenotype ------------------------------
    htn_dx_patients <-  import(here::here("data","clean","htn_dx_patients.csv"))
    htn_meds_patients <- import(here::here("data","clean","htn_meds_patients.csv"))
    
  analytical2 <- analytical %>% 
    left_join(htn_dx_patients, by = "PATIENT_LINKAGE")
  
  analytical3 <- analytical2 %>% 
    left_join(htn_meds_patients, by = "PATIENT_LINKAGE") %>% 
    distinct(PATIENT_LINKAGE, .keep_all = TRUE)
  
    
# browse the merged file
  skim(analytical3)
  head(analytical3, 10)
  
  
# replace missing DX and MEDS with 0
  analytical3$DX <- as.numeric(analytical3$HTN_DX)
    analytical3$DX[is.na(analytical3$DX)] <- 0

  analytical3$MEDS <- as.numeric(analytical3$MEDS)
    analytical3$MEDS[is.na(analytical3$MEDS)] <- 0
    
    
  
 # Hypertension e-phenotype based on BP 140/90 ----
    
  analytical3$hypertension_140 <- ifelse((analytical3$DX == 1 |  
                                            analytical3$MEDS == 1 |  
                                            analytical3$HTN140_90 == 1),
                                            1,0 )
  
# Hypertension e-phenotype based on BP 130/80 ----
    
  analytical3$hypertension_130 <- ifelse((analytical3$DX == 1 |  
                                            analytical3$MEDS == 1 |  
                                            analytical3$HTN130_80 == 1),
                                            1,0 )
 
  # browse this new subset: 
    str(analytical3)
    head(analytical3, 10)
    
    analytical3 %>% 
      select(PATIENT_LINKAGE, HTN140_90, DX, MEDS, hypertension_140) %>% 
      slice_head(n = 10) 
    
    analytical3 %>% 
      select(PATIENT_LINKAGE, HTN130_80, DX, MEDS, hypertension_130) %>% 
      slice_head(n = 10) 
     
  
  analytical_htn <- analytical3 %>% 
    select(-c(CONDITION_CONCEPT_ID, CONDITION_START_DATE, HTN_DX,
              DRUG_CONCEPT_ID, DRUG_EXPOSURE_START_DATE)) 
  
 
  ## look at this final analytical subset:  
      head(analytical_htn, 10)
      analytical_htn %>% tabyl(hypertension_140) %>% 
        adorn_totals(where = "row")
      
      analytical_htn %>% tabyl(hypertension_130) %>% 
        adorn_totals(where = "row")
      

      
## Step 7. Export for future sharing and using:
      
  #export(analytical_htn, here("data","clean","20230914_AEMR2022_analytical_htn.csv"))
  
    
  export(analytical_htn, here("data","clean","20230914_AEMR2022_analytical_htn_kb.csv"))
  
  
  
  
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#
  
## Archived ----
  
  # Slow codes with Tidyverse:
  # all_pop_merged8 <- analytical %>% 
  #   mutate(
  #     hypertension140 = case_when(
  #     HTN_DX == 1 |   MEDS ==1 |  hbp140 ==1  ~ 1,
  #     TRUE                                    ~ 0),
  #     
  #     hypertension130 = case_when(
  #       HTN_DX == 1 |   MEDS ==1 |  hbp130 ==1  ~ 1,
  #       TRUE                                    ~ 0)
  #   )
 
  
## Hypertension diagnosis codes can also be imported as such:
  # dx <- import(here("data","import","htn_dx_standard_codelist.xlsx"))
  # htn_dx_codelist <-paste(dx[[1]], collapse = ",") # separate the items with comma; do this outside of the SQL query step to keep the query simpler
  # 
  
## Antihypertensive medication codes can also be imported as such:
  # meds <- import(here("data","import","htn_meds_standard_codelist.xlsx"))
  # htn_meds_codeslit <- paste(meds[[1]], collapse = ",")