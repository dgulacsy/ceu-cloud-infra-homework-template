# Init env ----------------------------------------------------------------
rm(list=ls())

library(httr)
library(aws.s3)
library(jsonlite)
library(lubridate)

# Access Key setup ------
keyTable <- read.csv("accessKeys.csv", header = T) # accessKeys.csv == the CSV downloaded from AWS containing your Acces & Secret keys
AWS_ACCESS_KEY_ID <- as.character(keyTable$Access.key.ID)
AWS_SECRET_ACCESS_KEY <- as.character(keyTable$Secret.access.key)

#activate
Sys.setenv("AWS_ACCESS_KEY_ID" = AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY,
           "AWS_DEFAULT_REGION" = "eu-west-1")

# Set the date to get data for ---------
DATE_PARAM="2021-01-26"
date <- as.Date(DATE_PARAM, "%Y-%m-%d")

# Get data from wikipedia --------

# See https://wikimedia.org/api/rest_v1/#/Edited%20pages%20data/get_metrics_edited_pages_top_by_edits__project___editor_type___page_type___year___month___day_
url <- paste(
  "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/",
  format(date, "%Y/%m/%d"), sep='')

wiki.server.response = GET(url)
wiki.response.status = status_code(wiki.server.response)
wiki.response.body = content(wiki.server.response, 'text')

if (wiki.response.status != 200){
  print(paste("Recieved non-OK status code from Wiki Server: ",
              wiki.response.status,
              '. Response body: ',
              wiki.response.body, sep=''
  ))
}

# Save Raw Response and upload to S3 ----------
# Create Local file
RAW_LOCATION_BASE='data/raw-views'
dir.create(file.path(RAW_LOCATION_BASE), showWarnings = FALSE)

raw.output.filename = paste("raw-views-", format(date, "%Y-%m-%d"), '.txt',
                            sep='')

raw.output.fullpath = paste(RAW_LOCATION_BASE, '/', 
                            raw.output.filename, sep='')

write(wiki.response.body, raw.output.fullpath)

# Upload the file to S3 --------------

put_object(file = paste0("data/raw-views/raw-views-",format(date, "%Y-%m-%d"),".txt"),
           object = paste0("de4/raw/raw-views-",format(date, "%Y-%m-%d"),".txt"),
           bucket = "dominik-de4",
           verbose = TRUE)


# Parse the response and write the parsed string to "Bronze"

# We are extracting the top page views from the server's response
wiki.response.parsed = content(wiki.server.response, 'parsed')
top.views = wiki.response.parsed$items[[1]]$articles

# Convert the server's response to JSON lines
current.time = Sys.time() 
json.lines = ""
for (i in 1:length(top.views)){
  article = top.views[[i]]
  record = list(
    article = article$article[[1]],
    views = article$views,
    rank = article$rank,
    date = format(date, "%Y-%m-%d"),
    retrieved_at = current.time
  )
  
  json.lines = paste(json.lines,
                     toJSON(record,
                            auto_unbox=TRUE),
                     ifelse(i!=length(top.views),"\n",""),
                     collapse=NULL)
}

# Save the Top Views JSON lines as a file and upload it to S3

JSON_LOCATION_BASE='data/views'
dir.create(file.path(JSON_LOCATION_BASE), showWarnings = FALSE)

json.lines.filename = paste("views-", format(date, "%Y-%m-%d"), '.json',
                            sep='')
json.lines.fullpath = paste(JSON_LOCATION_BASE, '/', 
                            json.lines.filename, sep='')

cat(json.lines, file = json.lines.fullpath)

put_object(file = json.lines.fullpath,
           object = paste('de4/views/',
                          json.lines.filename,
                          sep = ""),
           bucket = "dominik-de4",
           verbose = TRUE)

