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

# FUNCTIONS FOR RTT EXTRACTION (JAN07-DEC10, JAN11-MAR13, APR13-TODAY) ---------
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

  #function for extraction January 2007 to December 2010
rtt_extraction_jan07_dec10 <- function(filepath) {
  filenames <- list.files(filepath, 
                          recursive = TRUE, full.names = TRUE)
  
  #extract excel sheets, which are called different variations of "Provider" over time
  rtt_list_summary <- lapply(filenames, 
                             function(x) {
                               sheet_name <- excel_sheets(x)[str_detect(excel_sheets(x), regex("^providers?$", ignore_case = TRUE))]
                               data.table(fname = str_extract(x, "[^/]+$"), 
                                          read_excel(path = x, sheet = sheet_name, na = c("-", "")))
                             })
                               
  #removing adjusted admitted waiting times (as in later data)
  rtt_list_summary <- lapply(rtt_list_summary, function(x) 
    if (any(sapply(1:6, function(i) str_detect(x[i, 2], "The 18 weeks rules|adjusted"))) & 
        !is.na(any(sapply(1:6, function(i) str_detect(x[i, 2], "The 18 weeks rules|\\badjusted\\b"))))) {
      return(NULL) 
    }
    else 
    {
      return(x)
    })
  
  rtt_list_summary <- Filter(Negate(is.null), rtt_list_summary)
  
  #adding dates (accounting for different file structures for later datasets)
  date_pattern <- "\\b(January|February|March|April|May|June|July|August|September|October|November|December)\\s\\d{4}\\b"
  rtt_list_summary <- lapply(rtt_list_summary, function(x) {
    if(names(x)[2] == "Title:"){
      date_found <- str_extract(x[3, 3], date_pattern)
      date_found <- date_found[!is.na(date_found)]
      x <- x |> 
        mutate(date = my(date_found))
      return(x)
    }
    else { 
      date_found <- sapply(1:2, function(i) str_extract(x[i, 2], date_pattern))
      date_found <- date_found[!is.na(date_found)]
      x <- x |> 
        mutate(date = my(date_found))
      return(x)
      }
  })

  #identifying pathway 
  rtt_list_summary <- lapply(rtt_list_summary, function(x) {
    if(names(x)[2] == "Title:"){
      x <- x |> 
        mutate(pathway = ifelse(str_detect(x[1, 3], "incomplete"), "incomplete", 
                                ifelse(str_detect(x[1, 3], "non-admitted"), "non-admitted", "admitted")), 
               pathway = str_replace_all(pathway, "-", "_"))
      return(x)
    }
    else { 
      pathway <- sapply(1:2, function(i) ifelse(str_detect(x[i, 2], "incomplete"), "incomplete", 
                                                ifelse(str_detect(x[i, 2], "non-admitted"), "non-admitted", 
                                                       ifelse(str_detect(x[i, 2], " admitted"), "admitted", NA))))
      pathway <- pathway[!is.na(pathway)]
      x <- x |> 
        mutate(pathway = pathway, 
               pathway = str_replace_all(pathway, "-", "_"))
      return(x)
    }
  })
  
  #dealing with column names 
  rtt_list_summary <- lapply(rtt_list_summary, function(x) {
    x <- x |> 
      row_to_names(row_number = "find_header")
    
    #setting pathway and date names as these disappeared with shifting up rows to be colnames
    setnames(x, names(x)[1], "fname")
    setnames(x, names(x)[ncol(x)], "pathway")
    setnames(x, names(x)[ncol(x) - 1], "date")
    
    #cleaning names overall
    setnames(x, names(x), make_clean_names(names(x)))
    
    #cleaning specific names which might differ among datasets 
    setnames(x, "x95th_percentile_waiting_time_in_weeks", "95th_percentile_waiting_time_in_weeks", skip_absent = TRUE)
    setnames(x, "x92nd_percentile_waiting_time_in_weeks", "92nd_percentile_waiting_time_in_weeks", skip_absent = TRUE)
    setnames(x, "code", "org_code", skip_absent = TRUE)
    setnames(x, "provider", "provider_name", skip_absent = TRUE)
    setnames(x, "provider_code", "org_code", skip_absent = TRUE)
    setnames(x, "provider_name", "org_name", skip_absent = TRUE)
    setnames(x, "total_known_clock_start", "total_number_of_completed_pathways_with_a_known_clock_start", skip_absent = TRUE)
    setnames(x, "total_known_clock_start_within_18_weeks", "total_with_a_known_clock_start_within_18_weeks", skip_absent = TRUE)
    setnames(x, "percent_within_18_weeks_column_bj_column_bi", "percent_within_18_weeks", skip_absent = TRUE)
    setnames(x, "percent_within_18_weeks_column_bi_column_bh", "percent_within_18_weeks", skip_absent = TRUE)
    setnames(x, "percent_within_18_weeks_column_bh_column_bg", "percent_within_18_weeks", skip_absent = TRUE)
    setnames(x, "sha", "sha_code", skip_absent = TRUE)
  
    #unique difference for november 2011 and incomplete
    if (unique(x$pathway) == "incomplete" & "total_with_a_known_clock_start_within_18_weeks" %in% names(x)) {
      setnames(x, "total_with_a_known_clock_start_within_18_weeks", "total_within_18_weeks")
    }
    
    #total variable depending on the pathway 
    if (unique(x$pathway) == "incomplete" & "total_all" %in% names(x)) {
      setnames(x, "total_all", "total_number_of_incomplete_pathways", skip_absent = TRUE)
    }
    else {
      setnames(x, "total_all", "total_number_of_completed_pathways_all", skip_absent = TRUE)
    }
    
    #removing sha_code 
    x <- x |> 
      select(-sha_code)
    
    #fixing range names
    old_names <- grep("^x", names(x), value = TRUE)  
    new_names <- str_replace(old_names, "^x", "between_")
    setnames(x, old_names, new_names)
})
  
  #adjusting the percent variable to be NA, not 0, if all other variables are 0
  rtt_list_summary <- lapply(rtt_list_summary, function(x) { 
    if (unique(x$pathway) == "incomplete" & "percent_within_18_weeks" %in% names(x)) { 
      x <- x |> 
        mutate(percent_within_18_weeks = ifelse(total_number_of_incomplete_pathways == 0 & percent_within_18_weeks == 0, 
                                                NA, percent_within_18_weeks))
    }
    else if (unique(x$pathway) != "incomplete" & "percent_within_18_weeks" %in% names(x)) {
      x <- x |> 
        mutate(percent_within_18_weeks = ifelse(total_number_of_completed_pathways_all == 0 & percent_within_18_weeks == 0, 
                                                NA, percent_within_18_weeks))
      
    }
    else {
      x <- x
    }
    })

  #renaming columns based on pathway 
  rtt_list_summary <- lapply(rtt_list_summary, function(x) { 
    pathway <- unique(x$pathway)
    names_to_change <- setdiff(names(x), c("fname", "org_code", "org_name", "treatment_function_code", "treatment_function", "date", "pathway"))
    new_names <- paste0(pathway, "_", names_to_change)
    setnames(x, names_to_change, new_names)
    
    for (name in new_names) {
      x[[name]] <- as.numeric(x[[name]])
    }
    
    return(x)
    })
  
  #sorting into separate lists based on pathways 
  pathways <- c("admitted", "non_admitted", "incomplete")
  rtt_results_jan07_dec10 <- list()
  
  for (pathway in pathways){
    rtt_results_jan07_dec10[[pathway]] <- list()
  }
  
  for (i in 1:(length(rtt_list_summary))) {
    pathway <- unique(rtt_list_summary[[i]]$pathway)
    rtt_results_jan07_dec10[[pathway]] <- rbind(rtt_results_jan07_dec10[[pathway]], rtt_list_summary[[i]], fill = TRUE)
  }
  
  #removing pathway variable
  rtt_results_jan07_dec10 <- lapply(rtt_results_jan07_dec10, function(x) {
    x <- x |> 
      select(-pathway)
    return(x)
    })
  
  rtt_results_jan07_dec10
}
    
  #between January 2011 and March 2013, the files were organised fundamentally differently
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

# EXTRACTING AND CLEANING RTT DATA (JANUARY 2007 UNTIL DECEMBER 2010) ----------
filepath <- file.path(getwd(), "rawdata/wait-times/before-2011")

rtt_results_jan07_dec10 <- rtt_extraction_jan07_dec10(filepath)

 # EXTRACTING AND CLEANING RTT DATA (JANUARY 2011 UNTIL MARCH 2013) -------------
filepath <- file.path(getwd(), "rawdata/wait-times/after-2011")
pathways <- c("admitted", "non_admitted", "incomplete")

  #creating list of results by waits pathway 
rtt_results_jan11_mar13 <- list()
for (pathway in pathways) {
  rtt_results_jan11_mar13[[pathway]] <- rtt_extraction_jan11_mar13(filepath, pathway)
}

# EXTRACTING AND CLEANING RTT DATA (APRIL 2013 UNTIL TODAY) --------------------
filepath <- file.path(getwd(), "rawdata/wait-times/after-2011")
pathways <- c("admitted", "non_admitted", "incomplete")

#creating list of results by waits pathway 
rtt_results_apr13_today <- list()
for (pathway in pathways) {
  rtt_results_apr13_today[[pathway]] <- rtt_extraction_apr13_today(filepath, pathway)
}

# LINKING JANUARY 2007 UNTIL TODAY ---------------------------------------------
rtt_results_jan07_today <- list()
for (pathway in pathways) {
  rtt_results_jan07_today[[pathway]] <- rbind(rtt_results_jan07_dec10[[pathway]], rtt_results_jan11_mar13[[pathway]], fill = TRUE)
  rtt_results_jan07_today[[pathway]] <- rbind(rtt_results_jan07_today[[pathway]], rtt_results_apr13_today[[pathway]], fill = TRUE)
  
  #removing average median waiting time in weeks and 95th percentile, which are missing in most cases anyways 
  rtt_results_jan07_today[[pathway]] <- rtt_results_jan07_today[[pathway]] |> 
    select(!contains("average_median_waiting_time")) |> 
    select(!contains("95th_percentile")) |> 
    select(!contains("92nd_percentile")) |> 
    select(!fname)
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
  #and turn it into first period with new arrangement, rather than date of period before change
    #unless it is a split
  change_indicator <- data_orgchanges |> 
    filter(!is.na(final_code)) |> 
    group_by(org_code, final_code) |> 
    mutate(change_date = max(date)) |> 
    mutate(change_date = ifelse(experiences_split == 0, change_date + months(1), change_date)) |> 
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
  
  data
}

rtt_results_jan07_today_orgadjusted <- list()

for (pathway in pathways) {
  rtt_results_jan07_today_orgadjusted[[pathway]] <- org_change_adjustment(rtt_results_jan07_today, pathway, trust_lookup_uncomplicated)
}

#OUTPUT RTT DATA BY PATHWAY ----------------------------------------------------
for (pathway in pathways) {
  #turn all character variables into doubles to save space 
  char_cols <- sapply(rtt_results_jan07_today_orgadjusted[[pathway]], is.character)
  for (name in names(rtt_results_jan07_today_orgadjusted[[pathway]])[char_cols]) {
    rtt_results_jan07_today_orgadjusted[[pathway]][[name]] <- factor(rtt_results_jan07_today_orgadjusted[[pathway]][[name]])
  }
  
  #output
  write.csv(rtt_results_jan07_today_orgadjusted[[pathway]], file.path(getwd(), paste0("data/wait-times/rtt_", pathway, "_jan07_today.csv")), row.names = FALSE)
}










