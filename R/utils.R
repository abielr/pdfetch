# Return end of annual date
year_end <- function(date) {
  as.Date(ISOdate(year(date),12,31))
}

# Return end of semiannual date
halfyear_end <- function(date) {
  lubridate::ceiling_date(date, "halfyear")-1
}

# Return end of quarter date
quarter_end <- function(date) {
  lubridate::ceiling_date(date, "quarter")-1
}

# Return end of month date
month_end <- function(date) {
  lubridate::ceiling_date(date, "month")-1
}