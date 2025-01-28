# NHS hospital data pipeline
Here you'll find code to extract and process NHS public hospital data. The NHS releases a lot of great data, but this is often hard for researchers to work with since it is fragmented across many different websites with non-standard file and variable names. The purpose of this repo is to allow researchers to quickly and easily produce hospital-level datasets across multiple years.

## What is in here already
- extract_supporting_facilities_main.py Pulls all the data files [Supporting facilities data](https://www.england.nhs.uk/statistics/statistical-work-areas/cancelled-elective-operations/supporting-facilities-data/) and saves it in rawdata/supporting-facilities/
- build_datasets_main.py Merges the raw data files into a hospital*time series and saves this in data/

## What to do if you want to add a new series to the repo
- Make a new branch
- Make a .py script for extracting all the data from the NHS page and saving in a new
- Make a .py script for merging all the raw data files and producing a hospital*time dataset
- The repo is set up so that only the derived dataset is tracked by the repo. The user can download all the raw data to their local copy if they want to, but the rawdata/ folder is ignored in the git.

## What I'm hoping for eventually
- User clones copy of the dataset that contains the extracted public NHS datasets. (If the file sizes get too big, we might need to rethink storage)
- User specifies inputs (variable names, first year, last year, data at the financial year/calendar year/quarter/month level)
- User runs Python script which pulls all variables for the relevant years and adjusts the periodicity (e.g. averaging or summing quarterly level data if user wants annual data)

## Potential complications / additional functionality
- NHS organizations sometimes change code (e.g. if a hospital switches from one trust to another or two trusts merge). Usually the series will have the code at the time the series was produced. But we probably want the user to be able to link the same hospital through changes of ownership
- Some data is at the NHS trust level and some is at the site level. Might want to add functionality to specify this.
- Different datasets will have different subsets of NHS organisations in (e.g. some will include mental health trusts or ambulance trusts as well as acute care hospitals). We probably just want to keep everything in for now but find a way to flag this to users.

## Data series I want to pull in rough order 
1. [Supporting facilities data](https://www.england.nhs.uk/statistics/statistical-work-areas/cancelled-elective-operations/supporting-facilities-data/). 
- Number of ORs
- Number of ORs for day cases
2. [Available and occupied beds](https://www.england.nhs.uk/statistics/statistical-work-areas/bed-availability-and-occupancy/bed-data-overnight/)
- Available beds
- Occupied beds
- Percent occupancy
- Query: I can't find critical care beds here but I know I have downloaded them in the past. Let's touch base about them since I want them too
3. [Wait times](https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/) (also [2012 and earlier](https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/))
4. [CQC inspections](https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/)

## Other useful resources
- A lot of these data come from the central returns forms. There is a data dictionary here https://v2.datadictionary.nhs.uk/
- Service search API https://developer.api.nhs.uk/nhs-api/documentation/service-search-organisations
