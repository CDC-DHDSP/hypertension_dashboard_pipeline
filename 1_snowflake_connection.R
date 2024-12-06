###>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>###
###                                                            ###
###         PART 1: IQVIA Snowflake ODBC  Connection           ###
###                                                            ###
###     Data source: IQVIA AEMR-US OMOP (Feb 2024 release)     ###
###         Script Author: IQVIA colleagues |  Feb 2023        ###
###            Modified by: Siran He, CDC |  Oct 2024          ###
###                                                            ###
###>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>###



# Step 1. Load useful packages for this script ----------------------------
  library(pacman)

  pacman::p_load( 
    DBI,  # helps connecting R to database management systems, has functions for reading and writing tables
    odbc, # helps connect to a SQL server & query data, has tools to explore objects & columns in the database
    skimr, # browse a dataset
    tidyverse
  )

  
# Step 2. Turn warning off --------------------------------------------------------

  defaultw <- getOption("warn")
  options(warn = -1)

  
# Step 3. Retrive IQVIA Snowflake log-in Credentials -------------------------------------------------------------
  uid <-
    read.table(
      "C:\\Users\\yxj4\\OneDrive - CDC\\+My_Documents\\CDC\\3 DHDSP\\9 Data Science Upskilling - DSU\\2024-2025\\Snowflake\\snowflake_u.txt",  # Analyst's IQVIA Snowflake user id, change path as needed
      header = FALSE, sep = "\t"
    )[1, 1]
  
  pwd <-
    read.table(
      "C:\\Users\\yxj4\\OneDrive - CDC\\+My_Documents\\CDC\\3 DHDSP\\9 Data Science Upskilling - DSU\\2024-2025\\Snowflake\\snowflake_p.txt", # Analyst's IQVIA Snowflake password, change path as needed
      header = FALSE, sep = "\t"
    )[1, 1]

  
# Step 4. Set up connection variables ---------------------------------------------
  sf_driver     <- "SnowflakeDSIIDriver"
  sf_server     <- "hk63736.north-europe.azure.snowflakecomputing.com"
  sf_warehouse  <- "EXPORTS_CDC"
  sf_role_xw    <- "XWENG_EXPORTS"     # Siran He (SHE) exports in Snowflake; change as needed
  sf_schema     <- "DBO"
  

# Step 5. Establish connection ----------------------------------------------------

  con <- dbConnect(
    odbc::odbc(),
    driver = sf_driver,
    server = sf_server,
    warehouse = sf_warehouse,
    uid = uid,
    pwd = pwd,
    role = sf_role_xw,     # Siran He (SHE) exports in Snowflake; change as needed
    schema = sf_schema
  )


# Step 6. Quick checks of connected data elements ----------------------------------
  
  ## odbc package tools & quick checks of data contents
  
    odbcListObjects(con)  # Top level objects
    odbcListObjectTypes(con)  # Database structure
    dbListTables(con, schema = "DBO") # Show all tables that can be viewed in this schema
  
  ## Columns in a selected domain (in this example, the "PERSON" domain, or table)
    odbcListColumns(con, catalog="AEMR_OMOP_PCOS_ALL_ADULT_WOMEN_21SEP2023_152355_638", schema="DBO", table="PERSON")
    


# Step 7. Disconnect from the ODBC source and reset warnings ----------------------
  
  ## Do this AFTER analyses
    
dbDisconnect(con)
options(warn = defaultw)
print("Done")
       