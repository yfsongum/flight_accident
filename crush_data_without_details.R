##----------------------------------------------------------------------------------------------------##
##                     SCRAPER FOR THE AVIATION SAFETY NETWORK ACCIDENT DATABASE                      ##
##----------------------------------------------------------------------------------------------------##


## R version 3.4.3 (2017-11-30)

## Author: Lisa Hehnke (dataplanes.org | @DataPlanes)

## Data source: https://aviation-safety.net/database/


#-------#
# Setup #
#-------#

# Install and load packages using pacman
if (!require("pacman")) install.packages("pacman")
library(pacman)

p_load(data.table, magrittr, rvest, stringi, tidyverse)


#--------------------------#
# Create URLs for scraping #
#--------------------------#

# Create base URL for each year
year <- seq(from = 2024, to = 2024, 1) # data available from 1919 onwards
base_url <- paste0("https://aviation-safety.net/database/dblist.php?Year=", year)

# Get page numbers for each year
get_pagenumber <- function(url) {
  url %>%
    html_session(httr::user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36")) %>%
    html_nodes("div.pagenumbers") %>%
    html_nodes("a") %>%
    html_attr("href") %>%
    unlist() %>%
    stri_sub(-1) %>%  # 提取页码
    as.numeric() %>%
    sort() %>%
    tail(1) %>%       # 取最大页码
    ifelse(purrr::is_empty(.), 1, .)  # 若没有找到则返回1
}


pagenumbers <- unlist(lapply(base_url, get_pagenumber))

# Create full URLs for each year
accidents_year <- data.frame(year, pagenumbers)

accidents_year %<>% 
  uncount(pagenumbers) %>%
  group_by(year) %>%
  mutate(pagenumbers = 1:n())

urls <- paste0("https://aviation-safety.net/database/dblist.php?Year=", accidents_year$year, "&lang=&page=", accidents_year$pagenumbers)


#-----------------#
# Scrape database #
#-----------------#

# Scrape accident data
get_table <- function(url) {
  url %>%
    read_html() %>%
    html_table(header = TRUE, fill = TRUE) %>%
    .[[1]]   # 只提取第一个表格
}


aviationsafetynet_accident_data <- lapply(urls, get_table)

aviationsafetynet_accident_data <- lapply(urls, get_table)
aviationsafetynet_accident_data <- do.call(rbind, aviationsafetynet_accident_data)

# Remove empty variables (Var.7, pic) and rename variables (fat., cat)
aviationsafetynet_accident_data %<>%
  select(`acc. date`, type, `reg.`, operator, `fat.`, location, dmg) %>%
  rename(date = `acc. date`,
         registration = `reg.`,
         fatalities = `fat.`,
         damage = dmg)
# Export data
write.csv(aviationsafetynet_accident_data, "aviationsafetynet_accident_data.csv", row.names = FALSE)
