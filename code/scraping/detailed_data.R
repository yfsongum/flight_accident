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
library(pbapply)
library(rvest)
library(stringr)
library(dplyr)
library(purrr)


p_load(data.table, magrittr, rvest, stringi, tidyverse)


#--------------------------#
# Create URLs for scraping #
#--------------------------#

# Create base URL for each year
year <- seq(from = 2025, to = 2025, 1) # data available from 1919 onwards
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

get_table_with_links <- function(url) {
  page <- read_html(url)
  
  # 找到表格节点（根据实际页面可能需要调整选择器）
  table_node <- page %>% html_node("table")
  
  # 提取表格数据为数据框（注意：这里只获取文本内容）
  table_df <- table_node %>% html_table(header = TRUE, fill = TRUE)
  
  # 提取第一列中所有<a>标签的链接，即详情页链接
  # 注意：确保只提取数据行中的链接（一般不会把表头也提取出来）
  links <- table_node %>% html_nodes("td:nth-child(1) a") %>% html_attr("href")
  
  # 如果链接为相对链接，则后续爬取时需补全为完整 URL
  table_df$detail_link <- links
  
  return(table_df)
}

# 使用 lapply 分别爬取所有页面的表格数据
accidents_list <- lapply(urls, get_table_with_links)

# 将所有页面数据合并为一个数据框
accidents_df <- bind_rows(accidents_list)

# 检查数据，确保 detail_link 列已经存在
# names(accidents_df)
# head(accidents_df)
##---------------------------------------------------------------------------
parse_details_improved <- function(text) {
  # 定义目标字段列表
  keys <- c("Date", "Time", "Type", "Owner/operator", "Registration", "MSN", 
            "Year of manufacture", "Engine model", "Fatalities", "Other fatalities", 
            "Aircraft damage", "Category", "Location", "Phase", "Nature", 
            "Departure airport", "Destination airport", "Investigating agency", 
            "Confidence Rating", "Narrative")
  
  # 如果文本中没有 "Date:"，则假定开头即为日期信息，手动加上 "Date:" 前缀
  if (!grepl("Date:", text)) {
    text <- paste0("Date:", text)
  }
  
  # 在每个目标字段前插入换行符（如果前面没有换行），便于后续分割
  # 构造正则：匹配目标字段前没有换行符的情况
  pattern <- paste0("(?<!\\n)(", paste(keys, collapse="|"), "):")
  text <- gsub(pattern, "\n\\1:", text, perl=TRUE)
  
  # 按换行拆分文本
  lines <- unlist(strsplit(text, "\n"))
  lines <- trimws(lines)
  lines <- lines[lines != ""]
  
  result <- list()
  current_key <- NULL
  
  # 遍历每一行，判断是否以 "字段名:" 开头
  for (line in lines) {
    m <- regexec("^([^:]+):\\s*(.*)$", line)
    parts <- regmatches(line, m)[[1]]
    if (length(parts) >= 3) {
      key <- parts[2]
      value <- parts[3]
      # 如果识别到的键在目标列表中，则认为是新字段
      if (key %in% keys) {
        result[[key]] <- value
        current_key <- key
      } else {
        # 若该行的键不在目标列表中，则将整行作为前一字段的补充内容
        if (!is.null(current_key)) {
          result[[current_key]] <- paste(result[[current_key]], line, sep = " ")
        }
      }
    } else {
      # 没有匹配到键值格式，则视为上一字段的补充
      if (!is.null(current_key)) {
        result[[current_key]] <- paste(result[[current_key]], line, sep = " ")
      }
    }
  }
  
  # 确保所有目标键都有值，缺失的填 NA
  for (k in keys) {
    if (is.null(result[[k]])) result[[k]] <- NA
  }
  
  return(result)
}

# 改进后的详情页爬取函数，调用新的解析函数
get_detail_data <- function(link) {
  # 补全相对链接
  if (!str_detect(link, "^http")) {
    link <- paste0("https://aviation-safety.net", link)
  }
  
  page <- read_html(link)
  
  # 这里假设详情信息位于 div#content 中，如无匹配则取整个页面文本
  details_text <- page %>% html_nodes("div#content") %>% html_text()
  if (length(details_text) == 0) {
    details_text <- page %>% html_text()
  }
  
  # 合并所有文本为一个字符串
  details_text <- paste(details_text, collapse = " ")
  
  # 使用改进版函数解析详情
  parsed <- parse_details_improved(details_text)
  parsed$detail_link <- link  # 保存详情链接以便核对
  
  # 转换为单行数据框返回
  return(as.data.frame(as.list(parsed), stringsAsFactors = FALSE))
}

# 注意：请确保 accidents_df 已经包含 detail_link 列（详情页链接），否则需先从“acc. date”列中提取链接
detail_data_list <- pblapply(accidents_df$detail_link, function(link) {
  Sys.sleep(0.5)  # 延时1秒
  tryCatch({
    get_detail_data(link)
  }, error = function(e) {
    message("Error processing link: ", link)
    return(NULL)
  })
})

# 移除爬取失败的情况
detail_data_list <- detail_data_list[!sapply(detail_data_list, is.null)]

# 合并所有详情数据为一个数据框
all_details_df <- bind_rows(detail_data_list)

# 保存整合后的数据为 CSV 文件
write.csv(all_details_df, "accident_details.csv", row.names = FALSE)
#######################################################################################
##以下代码可以输出粗略数据
# aviationsafetynet_accident_data <- lapply(urls, get_table)
# 
# aviationsafetynet_accident_data <- lapply(urls, get_table)
# aviationsafetynet_accident_data <- do.call(rbind, aviationsafetynet_accident_data)
# 
# # Remove empty variables (Var.7, pic) and rename variables (fat., cat)
# aviationsafetynet_accident_data %<>%
#   select(`acc. date`, type, `reg.`, operator, `fat.`, location, dmg) %>%
#   rename(date = `acc. date`,
#          registration = `reg.`,
#          fatalities = `fat.`,
#          damage = dmg)
# # Export data
# write.csv(aviationsafetynet_accident_data, "aviationsafetynet_accident_data.csv", row.names = FALSE)
