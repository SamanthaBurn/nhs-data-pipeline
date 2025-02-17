###################### CLEANING WAIT TIMES NHS DATA ############################

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
#issues to clear up 
  #new RTT data items files from October 2015 to March 2016 
    #seem to be extra data for incomplete provider pathways, including on new clock starts and dta, etc. 
    #the dta doesn't seem to be continued, but the new clock starts appear in new periods datasets later on separately
  #January2013 files are not downloading because they appear to be hosted on an amazon server

# FUNCTION FOR RTT EXTRACTION (OVERALL PROVIDER) -------------------------------
  #function for variable name adjustment to reflect pathway 
name_pathway_adjustment <- function(list_element) {
  pathway <- str_extract(list_element$fname[1], "(?i)(admitted|non[-_ ]?admitted|incomplete)") |> 
    tolower() |>           
    str_replace_all("nonadmitted", "non_admitted") |> 
    str_replace_all("-", "_")   
  names_to_change <- setdiff(names(list_element), c("fname", "org_code", "org_name", "treatment_function_code", "treatment_function", "date"))
  new_names <- paste0(pathway, "_", names_to_change)
  
  setnames(list_element, names_to_change, new_names)
}

  #before April 2013, the files were organised fundamentally differently
    ##provider summary data in one sheet; provider specialty data with other variables in another sheet
    ##function for extraction before April 2013
rtt_extraction_jan11_mar13 <- function(filepath, pathway) {
  
  #subsetting to those filenames to extract data from 
  filenames <- list.files(filepath, 
                          recursive = TRUE, full.names = TRUE)
  filenames <- filenames[!(grepl("Adjusted", filenames) | grepl("Periods", filenames))]
  filenames <- filenames[my(str_extract(filenames, "[A-Z][a-z]{2}\\d{2}")) < my("April2013")]
  filenames <- filenames[str_extract(filenames, "(?i)(admitted|non[-_ ]?admitted|incomplete)")|> 
                           tolower() |> 
                           str_replace_all("nonadmitted", "non_admitted") |> 
                           str_replace_all("-", "_") == pathway]
  
  #extracting provider summary data 
  rtt_list_summary <- lapply(filenames, 
                     function(x) 
                       data.table(fname = str_extract(x, "[^/]+$"), 
                                  date = my(str_extract(x, "[A-Z][a-z]{2}\\d{2}")),
                                            read_excel(path = x, sheet = 1, skip = 13, na = c("-", ""))))
  
  #cleaning variables
  rtt_list_summary <- lapply(rtt_list_summary, 
                             function(x) {
                               setnames(x, names(x), make_clean_names(names(x)))
                               setnames(x, "x95th_percentile_waiting_time_in_weeks", "95th_percentile_waiting_time_in_weeks")
                               setnames(x, "provider_name", "org_name")
                               x <- x |> 
                                 select(-sha_code)
                               })
  
  #adding variables so can link with other provider-level specialty data 
  rtt_list_summary <- lapply(rtt_list_summary, 
                             function(x) {
                               x <- x |> 
                                 mutate(treatment_function_code = ifelse(pathway == "admitted", "AP999",
                                                                         ifelse(pathway == "non-admitted", "NP999", "IP999")), 
                                        treatment_function = "Total")
                             })
  
  #assigning pathway to names
  rtt_list_summary <- lapply(rtt_list_summary, 
                             function(x)
                               name_pathway_adjustment(x))
                               
  #rbindlisting 
  rtt_summary <- rbindlist(rtt_list_summary) |> 
    select(-fname)
  
  #extracting specialty provider data 
  rtt_list_specialties <- lapply(filenames, 
                             function(x) 
                               data.table(fname = str_extract(x, "[^/]+$"), 
                                          date = my(str_extract(x, "[A-Z][a-z]{2}\\d{2}")),
                                          read_excel(path = x, sheet = 2, skip = 13, na = c("-", ""))))
  
  #cleaning variables
  rtt_list_specialties <- lapply(rtt_list_specialties, 
                             function(x) {
                               setnames(x, names(x), make_clean_names(names(x)))
                               setnames(x, "provider_name", "org_name")
                               old_names <- grep("^x", names(x), value = TRUE)  
                               new_names <- str_replace(old_names, "^x", "between_")
                               setnames(x, old_names, new_names)
                               x <- x |> 
                                 select(-sha_code)
                             })
  
  #assigning pathway to names 
  rtt_list_specialties <- lapply(rtt_list_specialties, 
                                 function(x) 
                                   name_pathway_adjustment(x))
  
  #rbindlisting 
  rtt_specialties <- rbindlist(rtt_list_specialties) |> 
    select(-fname)
  
  #merging summary and specialties 
  rtt_overall <- join(rtt_specialties, rtt_summary)
  
  rtt_overall
}

  #function for extraction from April 2013 onwards 
rtt_extraction_apr13_today <- function(filepath, pathway) {
  
  #subsetting to those filenames to extract data from 
  filenames <- list.files(filepath, 
                          recursive = TRUE, full.names = TRUE)
  filenames <- filenames[!(grepl("Adjusted", filenames) | grepl("Periods", filenames)| grepl("New_RTT_data_items", filenames))]
  filenames <- filenames[my(str_extract(filenames, "[A-Z][a-z]{2}\\d{2}")) >= my("April2013")]
  filenames <- filenames[str_extract(filenames, "(?i)(admitted|non[-_ ]?admitted|incomplete)")|> 
                           tolower() |> 
                           str_replace_all("nonadmitted", "non_admitted") |> 
                           str_replace_all("-", "_") == pathway]
  
  #extracting provider summary data 
  rtt_list_specialties<- lapply(filenames, 
                             function(x) 
                               data.table(fname = str_extract(x, "[^/]+$"), 
                                          date = my(str_extract(x, "[A-Z][a-z]{2}\\d{2}")),
                                          read_excel(path = x, sheet = 1, skip = 13, na = c("-", ""))))
  
  #cleaning variables
  rtt_list_specialties <- lapply(rtt_list_specialties, 
                             function(x) {
                               setnames(x, names(x), make_clean_names(names(x)))
                               setnames(x, "x95th_percentile_waiting_time_in_weeks", "95th_percentile_waiting_time_in_weeks", skip_absent = TRUE)
                               setnames(x, "x92nd_percentile_waiting_time_in_weeks", "92nd_percentile_waiting_time_in_weeks", skip_absent = TRUE)
                               setnames(x, "provider_code", "org_code")
                               setnames(x, "provider_name", "org_name")
                               old_names <- grep("^x", names(x), value = TRUE)  
                               new_names <- str_replace(old_names, "^x", "between_")
                               setnames(x, old_names, new_names)
                             })
  
  #dealing with some datasets going up to more than 104 weeks (harmonising)
  rtt_list_specialties <- lapply(rtt_list_specialties, 
                                 function(x) {
                                   if ("total_52_plus_weeks" %in% names(x)) {
                                     between_cols <- grep("^between_(\\d+_\\d+)$", names(x), value = TRUE)
                                     to_remove <- between_cols[sapply(as.numeric(str_extract(between_cols, "(?<=between_)\\d+")), function(x) x >= 52)]
                                     x <- x |> 
                                       select(-all_of(to_remove))
                                     setnames(x, "total_52_plus_weeks", "between_52_plus")
                                     total_cols <- grep("^total_[0-9]|104", names(x), value = TRUE) #removing the other total cols, e.g. more than 62 weeks
                                     x <- x |> 
                                       select(-all_of(total_cols))
                                   }
                                   else {
                                     x <- x
                                   }
                                 })
  
  #removing region/sha/etc. as not common across years 
  rtt_list_specialties <- lapply(rtt_list_specialties, 
                                 function(x) {
                                   to_remove <- grep("form|region_code|nhs_region|sha|sha_code|area_team", names(x), value = TRUE)
                                   x <- x |> 
                                     select(-all_of(to_remove))
                                 })
  
  #assigning pathway to names
  rtt_list_specialties <- lapply(rtt_list_specialties, 
                                 function(x)
                                   name_pathway_adjustment(x))
  
  #rbindlisting 
  rtt_specialties <- rbindlist(rtt_list_specialties, fill = TRUE) |> 
    select(-fname)
  
  rtt_specialties
}


# EXTRACTING AND CLEANING RTT DATA (JANUARY 2011 UNTIL MARCH 2013) -------------
filepath <- file.path(getwd(), "rawdata/wait-times")
pathways <- c("admitted", "non_admitted", "incomplete")

  #creating list of results by waits pathway 
rtt_results_jan11_mar13 <- list()
for (pathway in pathways) {
  rtt_results_jan11_mar13[[pathway]] <- rtt_extraction_jan11_mar13(filepath, pathway)
}

# EXTRACTING AND CLEANING RTT DATA (APRIL 2013 UNTIL TODAY) --------------------
filepath <- file.path(getwd(), "rawdata/wait-times")
pathways <- c("admitted", "non_admitted", "incomplete")

#creating list of results by waits pathway 
rtt_results_apr13_today <- list()
for (pathway in pathways) {
  rtt_results_apr13_today[[pathway]] <- rtt_extraction_apr13_today(filepath, pathway)
}

# LINKING JANUARY 2011 UNTIL TODAY ---------------------------------------------
rtt_results_jan11_today <- list()
for (pathway in pathways) {
  rtt_results_jan11_today[[pathway]] <- rbind(rtt_results_jan11_mar13[[pathway]], rtt_results_apr13_today[[pathway]], fill = TRUE)
}

# ACCOUNTING FOR ORGANISATIONAL CHANGES ----------------------------------------
trust_lookup_uncomplicated <- read.csv("data/org-changes/trust_lookup_uncomplicated_changes.csv")

#can deal with the variables on total number of people/people waiting for certain number of weeks 
#can also deal with percent waiting less than 18 weeks 
#can get a proxy for median waiting time (though not exact, as data is in bins)
#cannot account for the percentile variables as data may be missing  

org_change_adjustment <- function(data_list, pathway, lookup_file) {
  
  #initialising data
  org_change_lookup <- lookup_file 
  data <- data_list[[pathway]]
  
  #naming the variables that will be used later 
  percent_var <- paste0(pathway, "_percent_within_18_weeks")
  median_var <- paste0(pathway, "_average_median_waiting_time_in_weeks")
  total_var <- ifelse(
    pathway == "incomplete", 
    paste0(pathway, "_total_number_of_", pathway, "_pathways"), 
    paste0(pathway, "_total_number_of_completed_pathways_all")
  )
  
  
  #name file, as names will be taken out temporarily 
  name_code_lookup <- data |> 
    select(org_code, org_name) |> 
    unique() |> 
    group_by(org_code) |> 
    slice(1)
  
  data <- data |> 
    select(-org_name) #taking out names temporarily
  
  data$year <- as.numeric(data$year)
  
  #identifying the problematic trusts 
  problematic_trusts <- org_change_lookup |> 
    filter(problematic == 1)
  
  problematic_trusts <- unique(c(problematic_trusts$old_code, problematic_trusts$final_code)) 
  
  #creating variable to indicate problematic trusts 
  data <- data |> 
    mutate(exp_problematic_org_change = ifelse(org_code %in% problematic_trusts, 1, 0))
  
  #removing problematic trusts from lookup 
  org_change_lookup <- org_change_lookup |> 
    filter(problematic == 0) |> 
    select(-problematic)
  
  #separating out the affected trusts from beds 
  all_affected_trusts <- unique(c(org_change_lookup$old_code, org_change_lookup$final_code))
  data_orgchanges <- data |> 
    filter(org_code %in% all_affected_trusts)
  data <- data |> 
    filter(org_code %!in% all_affected_trusts)
  
  #linking on the changed trusts 
  setnames(org_change_lookup, "old_code", "org_code")
  data_orgchanges <- join(data_orgchanges, org_change_lookup)
  
  #getting indicator of period where something changed - can merge this back on later
  change_indicator <- data_orgchanges |> 
    filter(!is.na(final_code)) |> 
    group_by(org_code, final_code) |> 
    mutate(change_date = max(date)) |> 
    ungroup() |> 
    select(final_code, change_date, experiences_split) |> 
    unique() |> 
    rename(date = change_date, 
           org_code = final_code)

  #changing codes to final code 
  data_orgchanges <- data_orgchanges |> 
    mutate(org_code = ifelse(!is.na(final_code), final_code, org_code))
  
  #aggregating by trust, returning NA still if ALL values are NA 
  data_orgchanges <- data_orgchanges |> 
    group_by(date, org_code, treatment_function_code, treatment_function, year, exp_problematic_org_change) |> 
    summarise(across(contains(c("between", "total")), ~ifelse(all(is.na(.)), NA, sum(., na.rm=TRUE))))
  
  #making cumulative variables to get percent and median variables 
  cumulatives <- data_orgchanges |> 
    pivot_longer(cols = contains("between"), 
                 values_to = "count") |> 
    group_by(org_code, treatment_function, date) |> 
    mutate(count = as.numeric(count), 
           cum_freq = cumsum(count))
  
  #percent_within_18_weeks 
  cumulatives <- cumulatives |> 
    mutate(!!percent_var := ifelse(name == "incomplete_between_17_18" & count != 0, 
                                   cum_freq/!!sym(total_var), NA)) |> 
    group_by(date, org_code, treatment_function) |> 
    fill(!!sym(percent_var), .direction = "updown")
  
  #median_waits 
  cumulatives <- cumulatives |> 
    group_by(date, org_code, treatment_function) |> 
    mutate(cumulative_percent = cum_freq / !!sym(total_var),  
           !!median_var := ifelse(cumulative_percent >= 0.5 & lag(cumulative_percent) < 0.5, name, NA)) |> 
    mutate(!!median_var := ifelse(!is.na(!!sym(median_var)), 
                                  as.numeric(str_extract(!!sym(median_var), "(?<=_)[0-9]+")) + 0.5, NA)) |> 
    fill(!!sym(median_var), .direction = "updown")
  
  #turning cumulatives into a lookup 
  cumulatives <- cumulatives |> 
    select(date, org_code, treatment_function, treatment_function_code, !!sym(percent_var), !!sym(median_var)) |> 
    distinct()
  
  data_orgchanges <- join(data_orgchanges, cumulatives)
  
  #linking all back together 
  data <- rbind(data, data_orgchanges, fill = TRUE) |> 
    arrange(org_code, treatment_function, date) 
  
  #merging on information on changes and names 
  data <- join(data, name_code_lookup)
  data <- join(data, change_indicator)|> 
    rename(org_change = experiences_split) |> 
    mutate(org_change = ifelse(!is.na(org_change), 1, 0))
  
}

for (pathway in pathways) {
  org_change_adjustment(rtt_results_jan11_today, pathway, trust_lookup_uncomplicated)
}










