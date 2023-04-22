#################################################################            
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: send_valueset.R
# Description:
# Dependency: 
#################################################################

rm(list=ls()); gc()
setwd("C:/repos/pim_geriatric_cdm/")

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

tgt_schema <- "PIM2016"
tgt_tbl <- "PIM_VS_RXNORM"

# make db connection
sf_conn <- DBI::dbConnect(drv = odbc::odbc(),
                          dsn = Sys.getenv("ODBC_DSN_NAME"),
                          uid = Sys.getenv("SNOWFLAKE_USER"),
                          pwd = Sys.getenv("SNOWFLAKE_PWD"))

load_valueset(
    vs_template = "curated",
    vs_url = "https://raw.githubusercontent.com/sxinger/PheCDM/main/valuesets/valueset_curated/vs-osa-comorb.json",
    vs_name_str = cov_vec[i],
    dry_run = TRUE,
    conn=sf_conn,
    write_to_schema = tgt_schema,
    write_to_tbl = tgt_tbl,
    overwrite = FALSE
    
)

