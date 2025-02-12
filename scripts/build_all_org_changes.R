########################CREATING ORG CHANGES TRACKER############################

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

# DEALING WITH SUCCESSIVE CHANGES: OVERVIEW MAPPING TO TRACK CHANGES -----------
all_codes <- unique(c(successor_changes$old_code, successor_changes$new_code))
non_final_codes <- all_codes[all_codes %in% successor_changes$old_code]

#initial result mapping
mapping <- tibble(old_code = all_codes, new_code_0 = all_codes)

#get the indices of non-final units that need replacement 
repl <- which(mapping$new_code_0 %in% non_final_codes)

for (i in c(1:10)) {
  if (length(repl) > 0) {
    #create variable names for loop 
    previous_code_var <- paste0("new_code_", i-1)
    new_code_var <- paste0("new_code_", i)
    date_change <- paste0("date_change_", i)
    change_type <- paste0("change_type_", i)
    
    #create variables 
    mapping <- mapping |> 
      mutate(
        !!new_code_var := NA_character_, 
        !!date_change := as.Date(NA)    
      )
    
    mapping[[date_change]] <- vector("list", nrow(mapping))
    
    #build vector to find the true next successor for a new code
    repl_v <- sapply(repl, function(x) successor_changes$new_code[successor_changes$old_code == mapping[[previous_code_var]][x]])
    repl_date <- sapply(repl, function(x) as.Date(successor_changes$succession_effective_date[successor_changes$old_code == mapping[[previous_code_var]][x]]))
    
    #replace non-final codes with the actual next successor
    mapping[[new_code_var]][repl] <- repl_v
    mapping[[date_change]][repl] <- repl_date
    
    #determining the type of change 
    mapping <- mapping |> 
      mutate(!!change_type := ifelse(lengths(!!sym(new_code_var)) > 1, "split", 
                                     ifelse(is.na(!!sym(new_code_var)), NA, "merger_or_name_change")))
    
    #unnesting splits 
    mapping <- mapping |> 
      unnest(c(!!sym(new_code_var), !!sym(date_change)))
    
    #indices of those trusts which are still not final 
    repl <- which(mapping[[new_code_var]] %in% non_final_codes)
  }
  
  else {
    break
  }
  
}

mapping <- mapping |> 
  mutate(date_change_3 = as.Date(date_change_3)) |> 
  filter(!is.na(new_code_1))

#identifying those paths which are complete, i.e. which are not part of a larger path 
new_codes <- unique(c(mapping$new_code_1, mapping$new_code_2, mapping$new_code_3))
mapping <- mapping |> 
  mutate(original_path = ifelse(old_code %!in% new_codes, 1, 0))

#WORKING ONLY WITH ORIGINAL PATHS TO IDENTIFY PROBLEMATIC ONES -----------------
mapping_only_originals <- mapping |> 
  filter(original_path == 1)

#identifying name changes 
#is name change if the number of times the mapped to code appears is only one 
#unless a split happens at a later stage in time? 
for (i in c(1:3)) {
  previous_code_var <- paste0("new_code_", i-1)
  new_code_var <- paste0("new_code_", i)
  date_change <- paste0("date_change_", i)
  change_type <- paste0("change_type_", i)
  
  #identifying those codes which are involved in mergers 
  #(they could also be classed as change_type = split if a trust splits into one of these)
  mergers <- mapping_only_originals |> 
    filter(!!sym(change_type) == "merger_or_name_change") |> 
    pull(!!sym(new_code_var))
  
  name_changed_codes <- mapping_only_originals |> 
    filter(!!sym(new_code_var) %in% mergers) |> 
    group_by(!!sym(new_code_var)) |> 
    summarise(
      total_occurrences = n(), 
      unique_old_codes = n_distinct(!!sym(previous_code_var))
    ) |> 
    filter(unique_old_codes == 1) |> 
    pull(!!sym(new_code_var))
  
  mapping_only_originals <- mapping_only_originals |> 
    mutate(!!change_type := ifelse(!!sym(new_code_var) %in% name_changed_codes, "name_change", !!sym(change_type))) |> 
    mutate(!!change_type := ifelse(!!sym(change_type) == "merger_or_name_change", "merger", !!sym(change_type)))
}

#getting final code variable to show code at end of chain 
mapping_only_originals <- mapping_only_originals |> 
  mutate(final_code = ifelse(is.na(new_code_2) & is.na(new_code_3), new_code_1, 
                             ifelse(is.na(new_code_3), new_code_2, new_code_3)))

#IDENTIFYING COMPLICATED CASES -------------------------------------------------
mapping_only_originals <- mapping_only_originals |>
  rowwise() |> 
  mutate(experiences_split = ifelse("split" %in% c(change_type_1, change_type_2, change_type_3), 1, 0))

#now filtering for those trust codes that appear as split 
#don't just filter for split because a trust could also get merged into a larger trust that is part of a split
#e.g. A into C, and B into C and D. then we want to include A as complicated too. (ex: 5C6 and 5C7)
#also defining direct_path_complicated as trust codes which are involved in directly complicated paths
#and defining adjacent_to_complicated in order to identify trusts involved in complicated network 
linked_to_split <- unique(c(mapping_only_originals$old_code[mapping_only_originals$experiences_split == 1], 
                            mapping_only_originals$new_code_1[mapping_only_originals$experiences_split == 1], 
                            mapping_only_originals$new_code_2[mapping_only_originals$experiences_split == 1], 
                            mapping_only_originals$new_code_3[mapping_only_originals$experiences_split == 1]))
linked_to_split <- linked_to_split[!is.na(linked_to_split)]

mapping_only_complicated <- mapping_only_originals |> 
  filter(old_code %in% linked_to_split |
           new_code_1 %in% linked_to_split |
           new_code_2 %in% linked_to_split | 
           new_code_3 %in% linked_to_split) |> 
  mutate(direct_path_complicated = ifelse("merger" %in% c(change_type_1, change_type_2, change_type_3) &
                                            "split" %in% c(change_type_1, change_type_2, change_type_3), "later_merger", 
                                          ifelse("name_change" %in% c(change_type_1, change_type_2, change_type_3) &
                                                   "split" %in% c(change_type_1, change_type_2, change_type_3), "later_name_change", "no")), 
         adjacent_to_complicated = ifelse("split" %!in% c(change_type_1, change_type_2, change_type_3), 1, 0))

#identifying those paths where a trust is the result of multiple trusts splitting into it
for (i in c(1:3)) {
  previous_code_var <- paste0("new_code_", i-1)
  new_code_var <- paste0("new_code_", i)
  date_change <- paste0("date_change_", i)
  change_type <- paste0("change_type_", i)
  
  split_from_multiple <- mapping_only_complicated |> 
    filter(!!sym(change_type) == "split") |>
    filter(direct_path_complicated == "no") |> 
    group_by(!!sym(new_code_var)) |> 
    summarise(
      total_occurrences = n(), 
      unique_old_codes = n_distinct(!!sym(previous_code_var))) |> 
    filter(unique_old_codes > 1) |> 
    pull(!!sym(new_code_var))
  
  mapping_only_complicated <- mapping_only_complicated |> 
    mutate(direct_path_complicated = ifelse(!!sym(new_code_var) %in% split_from_multiple & 
                                              adjacent_to_complicated == 0, "split_from_multiple", direct_path_complicated))
}

#from this, we know that any trust codes which are not adjacent to complicated AND which don't have a complicated direct path are fine 
#identifying these for later - these will be splits that are treated as mergers directly 
not_complicated_splits <- mapping_only_complicated |> 
  group_by(final_code) |> 
  filter(!any(adjacent_to_complicated == 1) & 
           all(direct_path_complicated == "no")) |> 
  pull(final_code) |> 
  unique()

#flagging all final codes which are the result of a difficult branch at some point 
part_of_complicated_path <- mapping_only_complicated |> 
  filter(final_code %!in% not_complicated_splits) |> 
  pull(final_code) |> 
  unique()

mapping_only_complicated <- mapping_only_complicated |> 
  filter(final_code %in% part_of_complicated_path)

#OUTPUTTING LOOKUP OF ALL PATHS INCLUDING INFORMATION ON COMPLICATED PATHS ----- 
mapping_all_complete_paths <- join(mapping_only_originals, mapping_only_complicated) |> 
  mutate(direct_path_complicated = ifelse(is.na(direct_path_complicated), "no", direct_path_complicated), 
         adjacent_to_complicated = ifelse(is.na(adjacent_to_complicated), 0, adjacent_to_complicated), 
         part_of_complicated_path = ifelse(final_code %in% part_of_complicated_path, 1, 0)) |> 
  select(-original_path)

write.csv(mapping_all_complete_paths, file.path(getwd(), "data/org_changes/all_org_changes_paths_2000_2018.csv"), row.names = FALSE)
