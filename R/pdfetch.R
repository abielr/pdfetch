#' Fetch data from Yahoo Finance
#' 
#' @param identifiers a vector of Yahoo Finance tickers
#' @param fields can be any of "open", "high", "low", "close", "volume", or "adjclose"
#' @param from a Date object or string in YYYY-MM-DD format. If supplied, only data on or after this date will be returned
#' @param to a Date object or string in YYYY-MM-DD format. If supplied, only data on or before this date will be returned
#' @return a xts object
#' @export
#' @seealso \url{https://finance.yahoo.com/}
#' @examples
#' \dontrun{
#' pdfetch_YAHOO(c("^gspc","^ixic"))
#' pdfetch_YAHOO(c("^gspc","^ixic"), "adjclose")
#' }
pdfetch_YAHOO <- function(identifiers, 
                          fields=c("open","high","low","close","volume","adjclose"),
                          from=as.Date("2007-01-01"),
                          to=Sys.Date()) {
  
  valid.fields <- c("open","high","low","close","volume","adjclose")
  
  if (!missing(from))
    from <- as.Date(from)
  if (!missing(to))
    to <- as.Date(to)
  
  if (missing(fields))
    fields <- valid.fields
  if (length(setdiff(fields,valid.fields)) > 0)
    stop(paste0("Invalid fields, must be one of ", valid.fields))
  
  results <- list()
  for (i in 1:length(identifiers)) {
    url <- paste0("http://chart.yahoo.com/table.csv?s=",identifiers[i],
                         "&c=", year(from),
                         "&a=", month(from)-1,
                         "&b=", day(from),
                         "&f=", year(to),
                         "&d=", month(to)-1,
                         "&e=", day(to))
    req <- GET(url)
    fr <- content(req, encoding="utf-8", col_types=readr::cols())
    x <- xts(fr[,match(fields, valid.fields)+1], as.Date(fr[[1]]))
    dim(x) <- c(nrow(x),ncol(x))
    if (length(fields)==1)
      colnames(x) <- identifiers[i]
    else
      colnames(x) <- paste(identifiers[i], fields, sep=".")
    results[[identifiers[i]]] <- x
  }
  
  storenames <- sapply(results, names)
  results <- do.call(merge.xts, results)
  colnames(results) <- storenames
  results
}

#' Fetch data from St Louis Fed's FRED database
#' 
#' @param identifiers a vector of FRED series IDs
#' @return a xts object
#' @export
#' @seealso \url{https://fred.stlouisfed.org/}
#' @examples
#' \dontrun{
#' pdfetch_FRED(c("GDPC1", "PCECC96"))
#' }
pdfetch_FRED <- function(identifiers) {  
  results <- list()
  for (i in 1:length(identifiers)) {
    
    url <- paste0("https://research.stlouisfed.org/fred2/series/",identifiers[i],"/downloaddata/",identifiers[i],".txt")
    req <- GET(url)
    fileLines <- readLines(textConnection(content(req, encoding="utf-8")))
    freq <- sub(",", "", strsplit(fileLines[6], " +")[[1]][2])
    skip <- grep("DATE", fileLines)[1]
    fr <- utils::read.fwf(textConnection(content(req, encoding="utf-8")), skip=skip, widths=c(10,20), na.strings=".", colClasses=c("character","numeric"))
    
    dates <- as.Date(fr[,1], origin="1970-01-01")

    if (freq == "Annual")
      dates <- year_end(dates)
    else if (freq == "Semiannual")
      dates <- halfyear_end(dates)
    else if (freq == "Quarterly")
      dates <- quarter_end(dates)
    else if (freq == "Monthly")
      dates <- month_end(dates)
    
    ix <- !is.na(dates)
    x <- xts(as.matrix(fr[ix,2]), dates[ix])
    dim(x) <- c(nrow(x),1)
    colnames(x) <- identifiers[i]
    results[[identifiers[i]]] <- x
  }
  
  do.call(merge.xts, results)
}

#' Fetch data from European Central Bank's statistical data warehouse
#' 
#' @param identifiers a vector of ECB series IDs
#' @return a xts object
#' @export
#' @seealso \url{http://sdw.ecb.europa.eu/}
#' @examples
#' \dontrun{
#' pdfetch_ECB("FM.B.U2.EUR.4F.KR.DFR.CHG")
#' }
pdfetch_ECB <- function(identifiers) {
  results <- list()
  for (i in 1:length(identifiers)) {
    req <- GET(paste0("http://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=",identifiers[i],"&type=csv"))
    tmp <- content(req, as="text", encoding="utf-8")
    fr <- utils::read.csv(textConnection(tmp), header=F, stringsAsFactors=F)[-c(1:5),]
    
    if (inherits(fr, "character"))
      stop(paste0("Series ", identifiers[i], " not found"))
    
    freq <- strsplit(identifiers[i], "\\.")[[1]][2]
    
    if (freq == "A") {
      dates <- as.Date(ISOdate(as.numeric(fr[,1]), 12, 31))
    } else if (freq == "H") {
      year <- as.numeric(substr(fr[,1], 1, 4))
      month <- as.numeric(substr(fr[,1], 6, 6))*6
      dates <- month_end(as.Date(ISOdate(year, month, 1)))
    } else if (freq == "Q") {
      dates <- quarter_end(as.Date(as.yearqtr(fr[,1])))
    } else if (freq == "M") {
      dates <- month_end(as.Date(as.yearmon(fr[,1], "%Y%b")))
    } else if (freq == "B" || freq == "D") {
      dates <- as.Date(fr[,1])
    } else {
      stop("Unsupported frequency")
    }
    
    x <- xts(as.matrix(suppressWarnings(as.numeric(fr[,2]))), dates, origin="1970-01-01")
    dim(x) <- c(nrow(x),1)
    colnames(x) <- identifiers[i]
    results[[identifiers[i]]] <- x
  }
  
  do.call(merge.xts, results)
}

# Download Eurostat DSD file
pdfetch_EUROSTAT_GETDSD <- function(flowRef) {
  url <- paste0("http://ec.europa.eu/eurostat/SDMX/diss-web/rest/datastructure/ESTAT/DSD_", flowRef)
  req <- GET(url, add_headers(useragent="RCurl"))
  doc <- xmlInternalTreeParse(content(req, as="text"))
  
  doc
}

#' Fetch description for a Eurostat dataset
#' @param flowRef Eurostat dataset code
#' @export
#' @examples
#' \dontrun{
#' pdfetch_EUROSTAT_DSD("namq_gdp_c")
#' }
pdfetch_EUROSTAT_DSD <- function(flowRef) {
  doc <- pdfetch_EUROSTAT_GETDSD(flowRef)
  concepts <- setdiff(unlist(getNodeSet(doc, "//str:Dimension/@id")), c("OBS_VALUE","OBS_STATUS","OBS_FLAG"))
  for (concept in concepts) {
    codelist_id <- unclass(getNodeSet(doc, paste0("//str:Dimension[@id='",concept,"']//str:Enumeration/Ref/@id"))[[1]])
    codes <- unlist(getNodeSet(doc, paste0("//str:Codelist[@id='",codelist_id,"']/str:Code/@id")))
    descriptions <- unlist(getNodeSet(doc, paste0("//str:Codelist[@id='",codelist_id,"']/str:Code/com:Name/text()")))
    
    max.code.length <- max(sapply(codes, nchar))
    
    print("")
    print(paste(rep("=", 50), collapse=""))
    print(concept)
    print(paste(rep("=", 50), collapse=""))
    for (j in 1:length(codes)) {
      print(sprintf(paste0("%-",max.code.length+5,"s %s"), codes[j], xmlValue(descriptions[[j]])))
    }
  }
}

#' Fetch data from Eurostat
#' 
#' Eurostat stores its statistics in data cubes, which can be browsed at
#' \url{http://ec.europa.eu/eurostat/data/database}. To access data, specify the name of a data cube and optionally filter it based on its dimensions. 
#' 
#' @param flowRef Eurostat dataset code
#' @param from a Date object or string in YYYY-MM-DD format. If supplied, only data on or after this date will be returned
#' @param to a Date object or string in YYYY-MM-DD format. If supplied, only data on or before this date will be returned
#' @param ... optional dimension filters for the dataset
#' @return a xts object
#' @export
#' @examples
#' \dontrun{
#' pdfetch_EUROSTAT("namq_gdp_c", FREQ="Q", S_ADJ="SWDA", UNIT="MIO_EUR", 
#'                           INDIC_NA="B1GM", GEO=c("DE","UK"))
#' }
pdfetch_EUROSTAT <- function(flowRef, from, to, ...) {
  arguments <- list(...)
  doc <- pdfetch_EUROSTAT_GETDSD(flowRef)
  concepts <- setdiff(unlist(getNodeSet(doc, "//str:Dimension/@id")), c("OBS_VALUE","OBS_STATUS","OBS_FLAG"))
  
  key <- paste(sapply(concepts, function(concept) {
    if (concept %in% names(arguments)) {
      paste(arguments[[concept]], collapse="+")
    } else {
      ""
    }
  }), collapse=".")
  
  if (!missing(from))
    from <- as.Date(from)
  if (!missing(to))
    to <- as.Date(to)
  
  if (!missing(from) && !missing(to))
    url <- paste0("http://ec.europa.eu/eurostat/SDMX/diss-web/rest/data/",flowRef,"/",key,"/?startPeriod=",from,"&endPeriod=",to)
  else if (!missing(from))
    url <- paste0("http://ec.europa.eu/eurostat/SDMX/diss-web/rest/data/",flowRef,"/",key,"/?startPeriod=",from)
  else if (!missing(to))
    url <- paste0("http://ec.europa.eu/eurostat/SDMX/diss-web/rest/data/",flowRef,"/",key,"/?endPeriod=",to)
  else
    url <- paste0("http://ec.europa.eu/eurostat/SDMX/diss-web/rest/data/",flowRef,"/",key)
  
  req <- GET(url, add_headers(useragent="RCurl"))
  doc <- xmlInternalTreeParse(content(req, as="text", encoding="utf-8"))
  
  results <- list()
  seriesSet <- getNodeSet(doc, "//generic:Series")
  
  if (length(seriesSet) == 0) {
    warning("No series found")
    return(NULL)
  }

  for (i in 1:length(seriesSet)) {
    series <- seriesSet[[i]]
    
    idvalues <- list()
    for (node in getNodeSet(series, "generic:SeriesKey/generic:Value", "generic"))
      idvalues[[xmlGetAttr(node, "id")]] <- xmlGetAttr(node, "value")
    id <- paste(sapply(concepts, function(concept) idvalues[[concept]]), collapse=".")
    
    freq <- xmlGetAttr(getNodeSet(series, "generic:SeriesKey/generic:Value[@id='FREQ']", "generic")[[1]], "value")
    
    if (freq == "A") {
      dates <- as.Date(ISOdate(as.numeric(unlist(getNodeSet(series, ".//generic:ObsDimension/@value", "generic"))),12,31))
    
    } else if (freq == "Q") {
      dates <- as.Date(as.yearqtr(
        sapply(
          unlist(getNodeSet(series, ".//generic:ObsDimension/@value", "generic")),
          function(x) paste(substr(x, 1, 4), substr(x, 7, 8), sep="-")
        )
      ))
      dates <- quarter_end(dates)
    } else if (freq == "M") {
      dates <- as.Date(as.yearmon(unlist(getNodeSet(series, ".//generic:ObsDimension/@value", "generic")), format="%Y-%m"))
      dates <- month_end(dates)
    } else if (freq == "D") {
      dates <- as.Date(unlist(getNodeSet(series, ".//generic:ObsDimension/@value", "generic")))
    } else {
      print(unlist(getNodeSet(series, ".//generic:ObsDimension/@value", "generic")))
      stop("Unsupported frequency")
    }
    
    values <- as.numeric(getNodeSet(series, ".//generic:ObsValue/@value", "generic"))
    
    x <- xts(values, dates)
    dim(x) <- c(nrow(x),1)
    colnames(x) <- id
    results[[i]] <- x
  }
  
  na.trim(do.call(merge.xts, results), is.na="all")
}

#' Fetch data from World Bank
#' 
#' @param indicators a vector of World Bank indicators
#' @param countries a vector of countrie identifiers, which can be 2- or
#'   3-character ISO codes. The special option "all" retrieves all countries.
#' @return a xts object
#' @export
#' @seealso \url{http://data.worldbank.org/}
#' @examples
#' \dontrun{
#' pdfetch_WB("NY.GDP.MKTP.CD", c("BR","MX"))
#' }
pdfetch_WB <- function(indicators, countries="all") {
  countries <- paste(countries, collapse=";")
  indicators <- paste(indicators, collapse=";")
  
  query <- paste0("http://api.worldbank.org/countries/",countries,"/indicators/",indicators,"?format=json&per_page=1000")
  req <- GET(query)
  x <- fromJSON(content(req, as="text"))[[2]]
  
  if (!inherits(x, "data.frame")) {
    warning("No series found")
    return(NULL)
  }
  
  results <- data.frame(indicator=paste(x$indicator$id, x$country$id, sep="."),
                  value=as.numeric(x$value),
                  date=as.Date(ISOdate(as.numeric(x$date), 12, 31))) # This dating won't always work, need to detect frequency
  results <- dcast(results, date ~ indicator)
  results <- na.trim(xts(subset(results, select=-date), results$date), is.na="all")
  results
}

#' Fetch data from the Bank of England Interactive Statistical Database
#' 
#' @param identifiers a vector of BoE series codes
#' @param from start date
#' @param to end date; if not given, today's date will be used
#' @return a xts object
#' @export
#' @seealso \url{http://www.bankofengland.co.uk/boeapps/iadb/}
#' @examples
#' \dontrun{
#' pdfetch_BOE(c("LPMVWYR", "LPMVWYR"), "2012-01-01")
#' }
pdfetch_BOE <- function(identifiers, from, to=Sys.Date()) {
  if (length(identifiers) > 300)
    stop("At most 300 series can be downloaded at once")
  
  from <- as.Date(from)
  to <- as.Date(to)
  
  url <- paste0("http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp?csv.x=yes",
                "&SeriesCodes=",paste(identifiers, collapse=","),
                "&CSVF=TN&VPD=Y&UsingCodes=Y",
                "&Datefrom=", format(from, "%d/%b/%Y"),
                "&Dateto=", format(to, "%d/%b/%Y"))
  
  tmp <- tempfile()
  utils::download.file(url, destfile=tmp, quiet=T)
  fr <- utils::read.csv(tmp, header=T)
  unlink(tmp)
  
  dates <- as.Date(fr[,1], "%d %b %Y")
  xts(fr[,-1], dates)
}

#' Fetch data from U.S. Bureau of Labor Statistics
#' 
#' @param identifiers a vector of BLS time series IDs
#' @param from start year
#' @param to end year. Note that the request will fail if this is a future year
#'   that is beyond the last available data point in the series.
#' @return a xts object
#' @export
#' @seealso \url{https://www.bls.gov/data/}
#' @examples
#' \dontrun{
#' pdfetch_BLS(c("EIUIR","EIUIR100"), 2005, 2010)
#' }
pdfetch_BLS <- function(identifiers, from, to) {
  if (!is.numeric(from) || !is.numeric(to))
    stop("Both from and to must be integers")
  
  if (to < from)
    stop("to must be greater than or equal to from")
  
  years <- seq(from, to, by=10)
  if (years[length(years)] != to || length(years) == 1)
    years <- c(years, to)
  
  results <- list()
  for (id in identifiers)
    results[[id]] <- NA
  
  for (i in 2:length(years)) {
    from <- years[i-1]+1
    to <- years[i]
    if (i == 2)
      from <- years[i-1]
    
    req <- list(seriesid=identifiers, startyear=unbox(from), endyear=unbox(to))
    resp <- POST("https://api.bls.gov/publicAPI/v1/timeseries/data/", body=req, encode="json")
    resp <- fromJSON(content(resp, as="text", encoding="utf-8"))
    
    if (resp$status != "REQUEST_SUCCEEDED")
      stop("Request failed")
    
    series <- resp$Results$series
    for (j in 1:length(identifiers)) {
      seriesID <- series$seriesID[j]
      if (length(series$data[[j]]) > 0)
        results[[seriesID]] <- rbind(results[[seriesID]], series$data[[j]])
    }
  }
  
  ix <- sapply(results, function(x) inherits(x, "data.frame"))
  
  if (!all(ix))
    warning(paste("No data found for series", identifiers[!ix], "in specified time range"))
  
  if (all(!ix))
    return(NULL)
  
  results <- results[ix]
  
  for (id in names(results)) {
    dat <- subset(results[[id]], period != 'M13')
    freq <- substr(dat$period[1], 1, 1)
    periods <- as.numeric(substr(dat$period, 2, 3))
    years <- as.numeric(dat$year)
    
    if (freq == "M")
      dates <- as.Date(ISOdate(years, periods, 1))
    else if (freq == "Q")
      dates <- as.Date(ISOdate(years, periods*3, 1))
    else if (freq == "A")
      dates <- as.Date(ISOdate(years, 12, 31))
    else
      stop(paste("Unrecognized frequency", freq))
    
    dates <- month_end(dates)
    
    results[[id]] <- xts(as.numeric(dat$value), dates)
    colnames(results[[id]]) <- id
  }
  
  identifiers <- identifiers[identifiers %in% names(results)]
  na.trim(do.call(merge.xts, results), is.na="all")[, identifiers]
}

#' Fetch data from the French National Institute of Statistics and Economic Studies (INSEE)
#' 
#' @param identifiers a vector of INSEE series codes
#' @return a xts object
#' @export
#' @seealso \url{https://www.insee.fr/}
#' @examples
#' \dontrun{
#' pdfetch_INSEE(c("000810635"))
#' }
pdfetch_INSEE <- function(identifiers) {
  results <- list()
  
  for (id in identifiers) {
    url <- paste0("http://www.bdm.insee.fr/bdm2/affichageSeries.action?idbank=",id)
    page <- tryCatch({
      req <- GET(url, add_headers("Accept-Language"="en-US,en;q=0.8"))
      content(req, as="text")
    }, warning = function(w) {
      
    })
    
    if (!is.null(page)) {
      doc <- htmlParse(page)
      dat <- readHTMLTable(doc)[[1]]
      names(dat) <- c(as.character(dat[1,1]), as.character(dat[1,2]), as.character(dat[1,3]))
      dat <- utils::tail(dat, -1)
      
      if (names(dat)[2] == "Month") {
        year <- as.numeric(as.character(dat[,1]))
        month <- as.character(dat[,2])
        dates <- as.Date(paste(year, month, 1), format="%Y %b %d")
      } else if (names(dat[2]) == "Quarter") {
        year <- as.numeric(as.character(dat[,1]))
        month <- as.numeric(as.character(dat[,2]))*3
        dates <- as.Date(ISOdate(year, month, 1))
      } else if (ncol(dat) == 2) {
        year <- as.numeric(as.character(dat[,1]))
        dates <- as.Date(ISOdate(year, 12, 31))
      } else {
        stop("Unrecognized frequency")
      }
      
      values <- as.numeric(gsub("[^-.0-9]", "", dat[,ncol(dat)]))
      dates <- month_end(dates)
      
      x <- xts(values, dates)
      colnames(x) <- id
      results[[id]] <- x 
    } else {
      warning(paste("Series", id, "not found"))
    }
  }
  
  if (length(results) == 0)
    return(NULL)
  
  na.trim(do.call(merge.xts, results), is.na="all")
}

#' Fetch data from the UK Office of National Statistics
#' 
#' The ONS maintains multiple data products; this function can be used to
#' retrieve data from the Time Series Explorer, see \url{https://www.ons.gov.uk/timeseriestool}
#' 
#' @param identifiers a vector of ONS series codes
#' @param dataset optional ONS dataset name, only used if a time series is available in multiple datasets.
#' @return a xts object
#' @export
#' @examples
#' \dontrun{
#' pdfetch_ONS(c("LF24","LF2G"), "lms")
#' }
pdfetch_ONS <- function(identifiers, dataset) {
  identifiers <- tolower(identifiers)
  
  results <- list()
  
  for (id in identifiers) {
    url <- paste0("http://www.ons.gov.uk/search?q=",id)
    resp <- GET(url)
    ret_url <- resp$url
    
    if (ret_url==url) {
      warning(paste0("Could not find series '", id, "' in ONS time series database"))
      next()
    }
    
    url <- paste0("https://www.ons.gov.uk/generator?format=csv&uri=/", parse_url(ret_url)$path)
    
    default_dataset <- utils::tail(stringr::str_split(url, "/")[[1]],1)
    
    alternate_nodes <- xml2::xml_attr(xml2::xml_find_all(content(resp), "//div[@id='othertimeseries']//a"), "href")
    alternate_urls <- list()
    for (alt_url in alternate_nodes) {
      dataset_name <- utils::tail(stringr::str_split(alt_url, "/")[[1]], 1)
      alternate_urls[[dataset_name]] <- alt_url
    }
    
    if (!missing(dataset)) {
      dataset <- tolower(dataset)
      if (dataset!=default_dataset) {
        if (dataset %in% names(alternate_urls)) {
          url <- paste0("https://www.ons.gov.uk/generator?format=csv&uri=", alternate_urls[[dataset_name]])
        } else {
          warning(paste0("Series '", id, "' not found in dataset '", dataset, "'. Using default dataset '", default_dataset, "' instead."))
        }
      }
    }

    fr <- content(GET(url), col_types=readr::cols())
    
    datesA <- grep("^[0-9]{4}$", fr[[1]])
    datesQ <- grep("^[0-9]{4} Q[1-4]$", fr[[1]])
    datesM <- grep("^[0-9]{4} [A-Z]{3}$", fr[[1]])
    
    dates <- NULL
    if (length(datesM) > 0) {
      dates <- as.Date(paste(fr[[1]][datesM],1), "%Y %b %d")
      dateix <- datesM
    } else if (length(datesQ) > 0) {
      y <- as.numeric(substr(fr[[1]][datesQ], 1, 4))
      m <- as.numeric(substr(fr[[1]][datesQ], 7, 7))*3
      dates <- as.Date(ISOdate(y, m, 1))
      dateix <- datesQ
    } else if (length(datesA) > 0) {
      dates <- as.Date(ISOdate(as.numeric(fr[[1]][datesA]), 12, 31))
      dateix <- datesA
    }
    
    if (!is.null(dates)) {
      dates <- month_end(dates)
      x <- xts(as.numeric(fr[[2]][dateix]), dates)
      colnames(x) <- toupper(id)
      results[[id]] <- x
    }
  }

  if (length(results) == 0)
    return(NULL)
  
  na.trim(do.call(merge.xts, results), is.na="all")
}

#' Fetch data from the US Energy Information Administration
#' @param identifiers a vector of EIA series codes
#' @param api_key EIA API key
#' @return a xts object
#' @export
#' @seealso \url{http://www.eia.gov/}
#' @examples
#' \dontrun{
#' pdfetch_EIA(c("ELEC.GEN.ALL-AK-99.A","ELEC.GEN.ALL-AK-99.Q"), EIA_KEY)
#' }
pdfetch_EIA <- function(identifiers, api_key) {
  results <- list()
  
  for (i in 1:length(identifiers)) {
    id <- identifiers[i]
    url <- paste0("http://api.eia.gov/series/?series_id=",id,"&api_key=",api_key)
    req <- GET(url)
    res <- fromJSON(content(req, as="text", encoding="utf-8"))
    
    if (is.null(res$request)) {
      warning(paste("Invalid series code",id))
      next
    }
    
    freq <- res$series$f
    dates <- res$series$data[[1]][,1]
    data <- as.numeric(res$series$data[[1]][,2])
    
    if (freq == "A") {
      dates <- as.Date(ISOdate(as.numeric(dates), 12, 31))
    } else if (freq == "Q") {
      y <- as.numeric(substr(dates, 1, 4))
      m <- 3*as.numeric(substr(dates, 6, 6))
      dates <- month_end(as.Date(ISOdate(y,m,1)))
    } else if (freq == "M") {
      y <- as.numeric(substr(dates, 1, 4))
      m <- as.numeric(substr(dates, 5, 6))
      dates <- month_end(as.Date(ISOdate(y,m,1)))
    } else if (freq == "W" || freq == "D") {
      dates <- as.Date(dates, "%Y%m%d")
    } else {
      warning(paste("Unrecognized frequency",freq,"for series",id))
    }
    
    x <- xts(rev(data), rev(dates))
    colnames(x) <- id
    results[[i]] <- x
  }
  
  if (length(results) == 0)
    return(NULL)
  
  na.trim(do.call(merge.xts, results), is.na="all")
}
