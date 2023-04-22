#################################################################            
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: extract.R
# Description:
# Dependency: 
#################################################################

rm(list=ls()); gc()
setwd("C:/repos/pim_geriatric_cdm")

# install.packages("pacman")
pacman::p_load(
  DBI,
  jsonlite,
  odbc,
  tidyverse,
  magrittr,
  dbplyr
)

# make db connection
sf_conn <- DBI::dbConnect(
  drv = odbc::odbc(),
  dsn = Sys.getenv("ODBC_DSN_NAME"),
  uid = Sys.getenv("SNOWFLAKE_USER"), 
  pwd = Sys.getenv("SNOWFLAKE_PWD")
)

dat<-tbl(sf_conn,in_schema("PUBLIC","PIM_CASE_CTRL_ASET2")) %>% collect()
saveRDS(dat,file="./final_aset.rds")

meta<-data.frame(colnm = colnames(dat),
                 stringsAsFactors = F)
write.csv(meta,file="./ref/metadata.csv",row.names = F)
