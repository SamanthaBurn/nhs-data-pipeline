########################CREATING TRUST CHANGE LOOKUP############################

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

# LOADING SUCCESSOR DATA  ------------------------------------------------------
successor_changes <- read.csv("rawdata/organisational-changes/successor_changes_extract_feb25.csv")

setnames(successor_changes, names(successor_changes), make_clean_names(names(successor_changes))) 
setnames(successor_changes, c("organisation_code", "successor_organisation_code"), c("old_code", "new_code"))

successor_changes$succession_effective_date <- ymd(successor_changes$succession_effective_date)

#filtering out data before 2000 and after 2018, and only including trusts
#(filtering out after 2018 because no more new PFIs after 2018)
successor_changes <- successor_changes |> 
  filter(year(succession_effective_date) >= 2000 & year(succession_effective_date) <= 2018) |> 
  filter(str_length(old_code) == 3)

# DEALING WITH SUCCESSIVE CHANGES ----------------------------------------------
all_codes <- unique(c(successor_changes$old_code, successor_changes$new_code))
non_final_codes <- all_codes[all_codes %in% successor_changes$old_code]

#initial result mapping
mapping <- tibble(old_code = all_codes, final_code = all_codes)

#get the indices of non-final units that need replacement 
repl <- which(mapping$final_code %in% non_final_codes)

while (length(repl) > 0) #as long as there are still non-final codes in mapping$final_code
{ 
  #build vector to find the true next successor for a new code
  repl_v <- sapply(repl, function(x) successor_changes$new_code[successor_changes$old_code == mapping$final_code[x]])
  
  #replace non-final codes with the actual next successor
  mapping$final_code[repl] <- repl_v
  
  #indices of those trusts which are still not final 
  repl <- which(mapping$final_code %in% non_final_codes)
}

#removing rows that map to themselves 
mapping <- mapping |> 
  filter(old_code != final_code)
mapping <- data.frame(mapping)

#CHECKING SPLITTING ORGANISATIONS ----------------------------------------------
split_orgs <- unique(mapping |> filter(str_length(final_code) > 3) |> pull(old_code))

beds0024 <- read.csv("data/available-and-occupied-beds/overnight_day_beds_2000_24_clean.csv")
trusts <- unique(beds0024$org_code)

length(trusts[trusts %in% split_orgs]) #40 organisations in our sample period for the beds data split  



