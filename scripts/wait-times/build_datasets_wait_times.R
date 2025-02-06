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

# FUNCTION FOR RTT EXTRACTION (OVERALL PROVIDER) -------------------------------
  #function for variable name adjustment to reflect pathway 
name_pathway_adjustment <- function(list_element) {
  pathway <- str_extract(list_element$fname[1], "(?i)admitted|non-admitted|incomplete") |> 
    tolower() |>                   
    str_replace_all("-", "_")   
  names_to_change <- setdiff(names(list_element), c("fname", "org_code", "provider_name", "treatment_function_code", "treatment_function", "date"))
  new_names <- paste0(pathway, "_", names_to_change)
  
  setnames(list_element, names_to_change, new_names)
}

  #before April 2013, the files were organised fundamentally differently
  #provider summary data in one sheet; provider specialty data with other variables in another sheet
  #function for extraction before April 2013
rtt_extraction_jan11_mar13 <- function(filepath, pathway) {
  
  #subsetting to those filenames to extract data from 
  filenames <- list.files(filepath, 
                          recursive = TRUE, full.names = TRUE)
  filenames <- filenames[!(grepl("Adjusted", filenames) | grepl("Periods", filenames))]
  filenames <- filenames[my(str_extract(filenames, "[A-Z][a-z]{2}\\d{2}")) < my("April2013")]
  filenames <- filenames[str_extract(filenames, "(?i)(admitted|non-admitted|incomplete)")|> 
                           tolower() |>                   
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
                               old_names <- grep("^x", names(x), value = TRUE)  
                               new_names <- str_replace(old_names, "^x", "more_than_")
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
}

  #from April 2013 onwards 

# EXTRACTING AND CLEANING RTT DATA (JANUARY 2011 UNTIL MARCH 2013) -------------
filepath <- file.path(getwd(), "rawdata/wait-times")
pathways <- c("admitted", "non_admitted", "incomplete")

  #creating list of results by waits pathway 
rtt_results_jan11_mar13 <- list()
for (pathway in pathways) {
  rtt_results[[pathway]] <- rtt_extraction_jan11_mar13(filepath, pathway)
}

