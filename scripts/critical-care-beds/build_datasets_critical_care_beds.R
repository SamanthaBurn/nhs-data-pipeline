########### CLEANING CRITICAL CARE BEDS NHS CAPACITY DATA ######################

# PACKAGES AND WORKING DIRECTORY ------------------------------------------------
library(sf)
library(plyr)
library(dplyr)
library(tidyverse)
library(sjmisc)
library(foreign)
library(parallel)
library(vroom)
library(MatchIt)
library(rlist)
library(stringr)
library(data.table)
library(lubridate)
library(operator.tools)
library(attempt)
library(zoo)
library(openxlsx)
library(readxl)
library(ggplot2)
library(readODS)
library(fastDummies)
library(httr)
library(haven)
library(fixest)
library(janitor)
library(cellranger)
library(scales)
library(devtools)
library(sp)
library(sf)
library(did)
library(ggpubr)
library(modelsummary)

setwd("/Users/claraschreiner/Desktop/nhs-data-pipeline")

# FUNCTION FOR CRITICAL CARE BED EXTRACTION 2000 - 2010 -----------------------
critical_beds_extraction_0210 <- function(filepath) {
  filenames <- list.files(filepath, 
                              recursive=TRUE, full.names=TRUE) 
  
  #extracting data (early 2010 data in different format)
  beds_list <- lapply(filenames, function(x) {
                        sheet <- "Level of care by Trust"
                        sheet_names <- excel_sheets(x)
                        
                        if (sheet %in% sheet_names) {
                          data.table(fname = str_extract(x, "[^/]+$"), 
                                     read_excel(path = x, sheet = sheet, na = c("-", "", "NULL")))
                        }
                        
                        else {
                          NULL
                        }
                        })
  
  beds_list <- Filter(Negate(is.null), beds_list)
  
  beds_list
}


# FUNCTION FOR CRITICAL CARE BED CLEANING 2000 - 2010 --------------------------
critical_beds_cleaning_0210 <- function(beds_list) {

    #identifying date and adding date variable 
  beds_list <- lapply(beds_list, 
                      function(x) {
                        date_pattern <- "\\b(January|February|March|April|May|June|July|August|September|October|November|December)\\s\\d{4}\\b"
                        
                        date_var_name <- names(x)[str_detect(names(x), "Open and staffed|Available adult critical")]
                        
                        date_found <- my(str_extract(date_var_name, date_pattern))
                        
                        x <- x |> 
                          mutate(date = date_found) 
                      
                      })
  
  #dealing with column names 
  beds_list <- lapply(beds_list, function(x) {
    x <- x |> 
      row_to_names(row_number = "find_header")
    
    setnames(x, names(x), make_clean_names(names(x)))
    setnames(x, c(names(x)[1], names(x)[ncol(x)]), c("fname", "date"))
    setnames(x, "org_id", "org_code", skip_absent = TRUE)
    setnames(x, "name", "org_name", skip_absent = TRUE)
    setnames(x, "year", "old_year", skip_absent = TRUE)
    
    #setting names so that linkable with later critical care bed data 
    setnames(x, "open_and_staffed_adult_critical_care_beds", "number_of_adult_critical_care_beds_open")
    
    #creating month and year variable
    x <- x |> 
      mutate(month = lubridate::month(ymd(date), abbr = FALSE, label = TRUE)) |> 
      mutate(year = lubridate::year(date))
  })
  
  #removing area_team/region/sha and year (range version)
  beds_list <- lapply(beds_list, function(x) {
    to_remove <- grep("form|ha|st_ha|region|sha|sha_code|dhsc|ha|old_year", names(x), value = TRUE)
    x <- x |> 
      select(-all_of(to_remove))
  })
  
  #cleaning variable values 
  beds_list <- lapply(beds_list, function(x) {
    x <- x |> 
      filter(!is.na(org_name))
    
    x <- x |> 
      mutate(org_name = toupper(org_name))
  })
  
  #selecting only common variables with later data 
  beds_list <- lapply(beds_list, function(x) {
    x <- x |> 
      select(org_code, org_name, number_of_adult_critical_care_beds_open, date, month, year)
    
  })
  
  #rbinding 
  critical_care_beds_0210 <- rbindlist(beds_list, use.names = TRUE) |>
    arrange(org_code, year, month)
  
  return(critical_care_beds_0210)
  
}


# FUNCTION FOR CRITICAL CARE BED EXTRACTION 2010 - 2020 ------------------------
critical_beds_extraction_1020 <- function(filepath) {
  filenames <- list.files(filepath, 
                              recursive=TRUE, full.names=TRUE)
  
  filenames <- filenames[!grepl("England", filenames)]
  
  #extracting data (early 2010 data in different format)
  month_pattern <- "January|February|March|April|May|June|July|August|September|October|November|December"
  year_pattern <- "\\d{4}-\\d{2}"
  beds_list <- lapply(filenames, 
                      function(x) 
                        if (grepl("2010-11", x) & grepl("August|September|October|November", x)) {
                          data.table(fname = str_extract(x, "[^/]+$"), 
                                     month = str_extract(x, month_pattern),
                                     year = str_extract(x, year_pattern),
                                     read_excel(path = x, sheet = "Critical Care Beds", skip = 7, na = c("-", "NULL")))
                        }
                      else { 
                        data.table(fname = str_extract(x, "[^/]+$"), 
                                   month = str_extract(x, month_pattern),
                                   year = str_extract(x, year_pattern),
                                   read_excel(path = x, sheet = "Critical Care Beds", skip = 14, na = c("-", "NULL")))
                        }
                        
  )
  
   return(beds_list)
}


# FUNCTION FOR CRITICAL CARE BED CLEANING 2010 - 2020---------------------------
critical_beds_cleaning_1020 <- function(beds_list) {
  
  #set variable row name 
  beds_list <- lapply(beds_list, function(x) {
    #removing columns that are all NA
    x <- x |> 
      select(where(~!all(is.na(.))))
  })
  
  #dealing with some having extra month and year variables 
  beds_list <- lapply(beds_list, function(x) {
    if ("Year" %in% names(x)) {
      x <- x |> 
        select(-Year, -Month)
    }
    else {
      x <- x
    }
  })
  
  #cleaning names 
  beds_list <- lapply(beds_list, function(x) {
    
    setnames(x, names(x), make_clean_names(names(x)))
    setnames(x, "org_id", "org_code", skip_absent = TRUE)
    setnames(x, "code", "org_code", skip_absent = TRUE)
    setnames(x, "name", "org_name", skip_absent = TRUE)
    
    #removing area_team/region/sha
    to_remove <- grep("form|region_code|nhs_region|region|sha|sha_code|area_team|dco_team", names(x), value = TRUE)
    x <- x |> 
      select(-all_of(to_remove))
    
    #standardising variable names 
    setnames(x, names(x)[6:15], 
             c("number_of_adult_critical_care_beds_open", "number_of_paediatric_intensive_care_beds_open", "number_of_neonatal_critical_care_cots_or_beds_open", 
               "number_of_adult_critical_care_beds_occupied", "number_of_paediatric_intensive_care_beds_occupied", "number_of_neonatal_critical_care_cots_or_beds_occupied", 
               "adult_critical_care_beds_percent_occupied", "paediatric_intensive_care_beds_percent_occupied", "neonatal_critical_care_cots_or_beds_percent_occupied", 
               "number_of_non_medical_critical_care_transfers"))
  })
  
  #cleaning missing rows 
  beds_list <- lapply(beds_list, function(x) {
    x <- x |> 
      filter(!is.na(org_name))
  })
  
  #cleaning variables 
  beds_list <- lapply(beds_list, function(x) {
    
    #upper casing the names 
    x <- x |> 
      mutate(org_name = toupper(org_name))
    
    #setting date variables properly (instead of having range for year)
    new_year_dates <- c("January", "February", "March")
    
    x <- x |> 
      mutate(year = ifelse(month %in% new_year_dates, 
                           as.numeric(str_extract(year, "[0-9]{2}$")) + 2000, 
                           str_extract(year, "^[0-9]{4}")), 
             date = my(paste0(month, year)))
    
    #fixing org_names 
    x <- x |> 
      mutate(org_name = ifelse(grepl("PRIMARY CARE TRUST", org_name), 
                               str_replace(org_name, "PRIMARY CARE TRUST", "PCT"), 
                               org_name))
    
    #setting percent_occupied to NA if there are none of this type of bed 
    types <- c("adult_critical_care_beds", "paediatric_intensive_care_beds", "neonatal_critical_care_cots_or_beds")
    
    for (type in types) {
      x <- x |> 
        mutate(!!paste0(type, "_percent_occupied") := ifelse(!!sym(paste0("number_of_", type, "_open")) == 0,
                                                          NA, 
                                                          !!sym(paste0(type, "_percent_occupied"))))
      return(x)
    } 
    
  })
  
  #rbinding everything together
  critical_care_beds_1020 <- rbindlist(beds_list, use.names = TRUE) |>
    select(-fname) |> 
    arrange(org_code, year, month)
  
  return(critical_care_beds_1020)
}

# EXTRACTING AND CLEANING CRITICAL CARE BEDS DATA 2000 - 2010 ------------------
filepath <- file.path(getwd(), "rawdata/critical-care-beds/before-2010")
critical_care_beds_0210 <- critical_beds_extraction_0210(filepath)
critical_care_beds_0210 <- critical_beds_cleaning_0210(critical_care_beds_0210)


# EXTRACTING AND CLEANING CRITICAL CARE BEDS DATA 2010 - 2020 ------------------
filepath <- file.path(getwd(), "rawdata/critical-care-beds/after-2010")
critical_care_beds_1020 <- critical_beds_extraction_1020(filepath)
critical_care_beds_1020 <- critical_beds_cleaning_1020(critical_care_beds_1020)


# LINKING BEDS DATA 2000 - 2024, ACCOUNTING FOR ORG CHANGES AND OUTPUTTING------
critical_care_beds_0220 <- rbind(critical_care_beds_0210, critical_care_beds_1020, fill = TRUE) |> 
  arrange(org_code, year, month)

#loading trust_lookup_uncomplicated (for now)
trust_lookup_uncomplicated <- read.csv("data/org-changes/trust_lookup_uncomplicated_changes.csv")

#getting name file, as names will be taken out temporarily 
name_code_lookup <- critical_care_beds_0220 |> 
  select(org_code, org_name) |> 
  unique() |> 
  group_by(org_code) |> 
  slice(1)

critical_care_beds_0220 <- critical_care_beds_0220 |> 
  select(-org_name) #taking out names temporarily

#making key variables numerical
critical_care_beds_0220$year <- as.numeric(critical_care_beds_0220$year)

names_to_make_numerical <- str_detect(names(critical_care_beds_0220), "care")
names_to_make_numerical <- names(critical_care_beds_0220)[names_to_make_numerical]

for(name in names_to_make_numerical) {
  critical_care_beds_0220[[name]] <- as.numeric(critical_care_beds_0220[[name]])
}

#identifying the problematic trusts 
problematic_trusts <- trust_lookup_uncomplicated |> 
  filter(problematic == 1)

problematic_trusts <- unique(c(problematic_trusts$old_code, problematic_trusts$final_code)) 

#creating variable to indicate problematic trusts 
critical_care_beds_0220 <- critical_care_beds_0220 |> 
  mutate(exp_problematic_org_change = ifelse(org_code %in% problematic_trusts, 1, 0))

#removing problematic trusts from lookup 
trust_lookup_uncomplicated <- trust_lookup_uncomplicated |> 
  filter(problematic == 0) |> 
  select(-problematic)

#separating out the affected trusts from beds 
all_affected_trusts <- unique(c(trust_lookup_uncomplicated$old_code, trust_lookup_uncomplicated$final_code))
critical_care_beds_0220_orgchanges <- critical_care_beds_0220 |> 
  filter(org_code %in% all_affected_trusts)
critical_care_beds_0220 <- critical_care_beds_0220 |> 
  filter(org_code %!in% all_affected_trusts)

#linking on the changed trusts 
setnames(trust_lookup_uncomplicated, "old_code", "org_code")
critical_care_beds_0220_orgchanges <- join(critical_care_beds_0220_orgchanges, trust_lookup_uncomplicated)

#getting indicator of period where something changed - can merge this back on later
change_indicator <- critical_care_beds_0220_orgchanges |> 
  filter(!is.na(final_code)) |> 
  arrange(org_code, year, month) |> 
  group_by(org_code, final_code) |> 
  mutate(change_date = max(date)) |>
  mutate(change_date = ifelse(experiences_split == 0, change_date + months(1), change_date)) |> 
  fill(change_date, .direction = "up") |> 
  ungroup() |> 
  select(final_code, change_date, experiences_split) |> 
  unique() |> 
  rename(date = change_date, 
         org_code = final_code) |> 
  mutate(date = as.Date(date))

#changing names to final name 
critical_care_beds_0220_orgchanges <- critical_care_beds_0220_orgchanges |> 
  mutate(org_code = ifelse(!is.na(final_code), final_code, org_code))

#aggregating by trust, returning NA still if ALL values are NA 
critical_care_beds_0220_orgchanges <- critical_care_beds_0220_orgchanges |> 
  group_by(year, month, org_code, date, exp_problematic_org_change) |> 
  summarise(across(ends_with(c("open", "s_occupied", "transfers")), ~ifelse(all(is.na(.)), NA, sum(., na.rm=TRUE))))

#adding the percent_occupied variables back in 
for (category in c("adult_critical_care_beds", "paediatric_intensive_care_beds", "neonatal_critical_care_cots_or_beds")) {
   available_var <- paste0("number_of_", category, "_open")
   occupied_var <- paste0("number_of_", category, "_occupied")
   percent_var <- paste0(category, "_percent_occupied")
    
   critical_care_beds_0220_orgchanges <- critical_care_beds_0220_orgchanges |> 
     mutate(!!percent_var := !!sym(occupied_var) / !!sym(available_var)) |> 
     mutate(!!percent_var := ifelse(!!sym(percent_var) == "NaN", NA, !!sym(percent_var)))
}

#linking all back together 
critical_care_beds_0220 <- rbind(critical_care_beds_0220, critical_care_beds_0220_orgchanges) |> 
  arrange(org_code, date) 

#merging on information on changes and names 
critical_care_beds_0220 <- join(critical_care_beds_0220, name_code_lookup)

#creating new variables 
  # - unproblematic_org_change to indicate exact period when unproblematic org_change happens 
  # - exp_unproblematic_org_change to flag trusts that undergo unproblematic org change at some point
critical_care_beds_0220 <- join(critical_care_beds_0220, change_indicator)|> 
  rename(unproblematic_org_change = experiences_split) |> 
  mutate(unproblematic_org_change = ifelse(!is.na(unproblematic_org_change), 1, 0)) |> 
  group_by(org_code) |> 
  mutate(exp_unproblematic_org_change = ifelse(any(unproblematic_org_change == 1), 1, 0))
  
#outputting
write.csv(critical_care_beds_0220, file.path(getwd(), "data/critical-care-beds/critical_care_beds_2002_20_clean.csv"), row.names = FALSE)


