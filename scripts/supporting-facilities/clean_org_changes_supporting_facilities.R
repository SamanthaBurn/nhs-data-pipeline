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

#LOADING AND HARMONISING SUPPORTING FACILITIES ---------------------------------
supporting_facilities <- read.csv("data/supporting-facilities/supporting-facilities_clean.csv")

#harmonising variables with other data and capitalising org_names
supporting_facilities <- supporting_facilities |> 
  select(-c(SHA, area_team_name, area_team_code, region_code, region_name)) |> 
  mutate(organisation_name = toupper(organisation_name))

#harmonising variable names for later merging
setnames(supporting_facilities, 
         c("year_var", "quarter_var", "organisation_code", "organisation_name"), 
         c("year", "quarter", "org_code", "org_name"))

#ACCOUNTING FOR ORGANISATIONAL CHANGES -----------------------------------------
#loading trust_lookup_uncomplicated (for now)
trust_lookup_uncomplicated <- read.csv("data/org-changes/trust_lookup_uncomplicated_changes.csv")

#getting name file, as names will be taken out temporarily 
name_code_lookup <- supporting_facilities |> 
  select(org_code, org_name) |> 
  unique() |> 
  group_by(org_code) |> 
  slice_tail(n = 1)

supporting_facilities <- supporting_facilities |> 
  select(-org_name) #taking out names temporarily

#turning key variables numeric 
  #(NAs returned by coercion come from "data not returned" for RTH in 2012)
supporting_facilities$year <- as.numeric(supporting_facilities$year)
supporting_facilities$nr_operating_theatres <- as.numeric(supporting_facilities$nr_operating_theatres)
supporting_facilities$nr_day_case_theatres <- as.numeric(supporting_facilities$nr_day_case_theatres)

#identifying the problematic trusts 
problematic_trusts <- trust_lookup_uncomplicated |> 
  filter(problematic == 1)

problematic_trusts <- unique(c(problematic_trusts$old_code, problematic_trusts$final_code)) 

#creating variable to indicate problematic trusts 
supporting_facilities <- supporting_facilities |> 
  mutate(exp_problematic_org_change = ifelse(org_code %in% problematic_trusts, 1, 0))

#removing problematic trusts from lookup 
trust_lookup_uncomplicated <- trust_lookup_uncomplicated |> 
  filter(problematic == 0) |> 
  select(-problematic)

#separating out the affected trusts from beds 
all_affected_trusts <- unique(c(trust_lookup_uncomplicated$old_code, trust_lookup_uncomplicated$final_code))
supporting_facilities_orgchanges <- supporting_facilities |> 
  filter(org_code %in% all_affected_trusts)
supporting_facilities <- supporting_facilities |> 
  filter(org_code %!in% all_affected_trusts)

#linking on the changed trusts 
setnames(trust_lookup_uncomplicated, "old_code", "org_code")
supporting_facilities_orgchanges <- join(supporting_facilities_orgchanges, trust_lookup_uncomplicated)

#getting indicator of period where something changed - can merge this back on later
#IMPORTANT: this is the period BEFORE the organisational change happens 
change_indicator <- supporting_facilities_orgchanges |> 
  filter(!is.na(final_code)) |> 
  group_by(org_code, final_code) |> 
  mutate(change_year = max(year)) |> 
  mutate(quarter = ifelse(!is.na(quarter), str_extract(quarter, "[0-9]+"), quarter)) |> 
  mutate(change_quarter = ifelse(year == change_year & !is.na(quarter), max(as.numeric(quarter), na.rm = TRUE), NA)) |> 
  fill(change_quarter, .direction = "up") |> 
  ungroup() |> 
  select(final_code, change_year, change_quarter, experiences_split) |> 
  unique() |> 
  rename(year = change_year, 
         quarter = change_quarter, 
         org_code = final_code) |> 
  mutate(quarter = ifelse(!is.na(quarter), paste0("Q", quarter), quarter))

#making the date the first period with the new organisational arrangement 
change_indicator <- change_indicator |> 
  mutate(year = ifelse(is.na(quarter) & experiences_split == 0, year + 1, year))

change_indicator <- change_indicator |> 
  mutate(date = ifelse(!is.na(quarter) & experiences_split == 0, paste0(year, quarter), NA)) |> 
  mutate(date = yq(date) + months(3)) |> 
  mutate(quarter = ifelse(!is.na(date), quarter(date), quarter), 
         year = ifelse(!is.na(date), year(date), year))|> 
  mutate(quarter = ifelse(!is.na(quarter), paste0("Q", quarter), quarter)) |> 
  select(-date)

#changing names to final name 
supporting_facilities_orgchanges <- supporting_facilities_orgchanges |> 
  mutate(org_code = ifelse(!is.na(final_code), final_code, org_code))

#aggregating by trust, returning NA still if ALL values are NA 
supporting_facilities_orgchanges <- supporting_facilities_orgchanges |> 
  group_by(year, quarter, org_code, exp_problematic_org_change) |> 
  summarise(across(c(nr_operating_theatres, nr_day_case_theatres), ~ifelse(all(is.na(.)), NA, sum(., na.rm=TRUE))))

#linking all back together 
supporting_facilities <- rbind(supporting_facilities, supporting_facilities_orgchanges) |> 
  arrange(org_code, year, quarter) 

#merging on information on changes and names 
supporting_facilities <- join(supporting_facilities, name_code_lookup)

#creating new variables 
# - unproblematic_org_change to indicate exact period when unproblematic org_change happens 
# - exp_unproblematic_org_change to flag trusts that undergo unproblematic org change at some point
supporting_facilities <- join(supporting_facilities, change_indicator)|> 
  rename(unproblematic_org_change = experiences_split) |> 
  mutate(unproblematic_org_change = ifelse(!is.na(unproblematic_org_change), 1, 0)) |> 
  group_by(org_code) |> 
  mutate(exp_unproblematic_org_change = ifelse(any(unproblematic_org_change == 1), 1, 0))

#outputting
write.csv(supporting_facilities, file.path(getwd(), "data/supporting-facilities/supporting-facilities_clean_org_change_adj.csv"), row.names = FALSE)





