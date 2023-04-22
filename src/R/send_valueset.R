#################################################################            
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: send_valueset.R
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
  tidyr,
  magrittr,
  dbplyr,
  devtools,
  jsonlite
  )

source_url("https://raw.githubusercontent.com/sxinger/utils/master/extract_util.R")

tgt_schema <- "PUBLIC"
tgt_tbl <- "PIM_VS_RXNORM"

# make db connection
sf_conn <- DBI::dbConnect(
  drv = odbc::odbc(),
  dsn = Sys.getenv("ODBC_DSN_NAME"),
  uid = Sys.getenv("SNOWFLAKE_USER"),
  pwd = Sys.getenv("SNOWFLAKE_PWD")
)

# retain a local copy
dt<-load_valueset(
  vs_template = "vsac",
  vs_url = "https://raw.githubusercontent.com/RWD2E/phecdm/main/res/valueset_autogen/pim-rx.json",
  dry_run = TRUE,
  conn=sf_conn,
  write_to_schema = tgt_schema,
  write_to_tbl = tgt_tbl,
  overwrite = TRUE
) %>%
  mutate(CODE_LABEL = gsub(",",";",CODE_LABEL),
         CODEGRP_LABEL = gsub(",",";",CODEGRP_LABEL))
write.csv(dt,file="./ref/pim_vs_rxnorm.csv",row.names = FALSE)

# push to remote db
load_valueset(
  vs_template = "vsac",
  vs_url = "https://raw.githubusercontent.com/RWD2E/phecdm/main/res/valueset_autogen/pim-rx.json",
  dry_run = FALSE,
  conn=sf_conn,
  write_to_schema = tgt_schema,
  write_to_tbl = tgt_tbl,
  overwrite = TRUE,
  file_encoding = "utf-8"
)
