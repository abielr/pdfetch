#' Fetch data from Yahoo Finance
#' 
#' @param identifiers a vector of Yahoo Finance tickers
#' @param fields can be any of "open", "high", "low", "close", "volume", or "adjclose"
#' @param from a Date object or string in YYYY-MM-DD format. If supplied, only data on or after this date will be returned
#' @param to a Date object or string in YYYY-MM-DD format. If supplied, only data on or before this date will be returned
#' @return a xts object
#' @export
#' @examples
#' pdfetch_YAHOO(c("^gspc","^ixic"))
#' pdfetch_YAHOO(c("^gspc","^ixic"), "adjclose")
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
    tmp <- tempfile()
    download.file(paste0("http://chart.yahoo.com/table.csv?s=",identifiers[i],
                         "&c=", year(from),
                         "&a=", month(from)-1,
                         "&b=", day(from),
                         "&f=", year(to),
                         "&d=", month(to)-1,
                         "&e=", day(to)
                         ), destfile=tmp, quiet=T)
    fr <- read.csv(tmp, header=T)
    unlink(tmp)
    x <- xts(fr[,match(fields, valid.fields)+1], as.Date(fr[, 1]))
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
#' @examples
#' pdfetch_FRED(c("GDPC1", "PCECC96"))
pdfetch_FRED <- function(identifiers) {  
  results <- list()
  for (i in 1:length(identifiers)) {
    
    tmp <- tempfile()
    download.file(paste0("http://research.stlouisfed.org/fred2/series/",identifiers[i],"/downloaddata/",identifiers[i],".txt"), destfile=tmp, quiet=T) 
    fileLines <- readLines(tmp)
    freq <- sub(",", "", strsplit(fileLines[6], " +")[[1]][2])
    skip <- grep("DATE", fileLines)[1]
    fr <- read.fwf(tmp, skip=skip, widths=c(10,20), na.strings=".", colClasses=c("character","numeric"))
    unlink(tmp)
    
    dates <- as.Date(fr[,1], origin="1970-01-01")

    if (freq == "Annual")
      dates <- year_end(dates)
    else if (freq == "Semiannual")
      dates <- halfyear_end(dates)
    else if (freq == "Quarterly")
      dates <- quarter_end(dates)
    else if (freq == "Monthly")
      dates <- month_end(dates)
    
    x <- xts(as.matrix(fr[,2]), dates)
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
#' @examples
#' pdfetch_ECB("FM.B.U2.EUR.4F.KR.DFR.CHG")
pdfetch_ECB <- function(identifiers) {
  results <- list()
  for (i in 1:length(identifiers)) {
    tmp <- tempfile()
    download.file(paste0("http://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=",identifiers[i],"&type=csv"), destfile=tmp, quiet=T) 
    fr <- read.csv(tmp, header=F, stringsAsFactors=F)[-c(1:5),]
    unlink(tmp)
    
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
      dates <- month_end(as.Date(as.yearmon(fr[,1])))
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
  tmp <- tempfile()
  download.file(url, destfile=tmp, quiet=T, method="curl")
  doc <- xmlInternalTreeParse(tmp)
  unlink(tmp)
  
  doc
}

#' Fetch description for a Eurostat dataset
#' @param flowRef Eurostat dataset code
#' @export
#' @examples
#' pdfetch_EUROSTAT_DSD("namq_gdp_c")
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
#' \url{http://epp.eurostat.ec.europa.eu/portal/page/portal/statistics/search_database}. To access data, specify the name of a data cube and optionally filter it based on its dimensions. 
#' 
#' @param flowRef Eurostat dataset code
#' @param from a Date object or string in YYYY-MM-DD format. If supplied, only data on or after this date will be returned
#' @param to a Date object or string in YYYY-MM-DD format. If supplied, only data on or before this date will be returned
#' @param ... optional dimension filters for the dataset
#' @return a xts object
#' @export
#' @examples
#' pdfetch_EUROSTAT("namq_gdp_c", FREQ="Q", S_ADJ="SWDA", UNIT="MIO_EUR", INDIC_NA="B1GM",
#'  GEO=c("DE","UK"))
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
  
  tmp <- tempfile()
  download.file(url, destfile=tmp, quiet=T, method="curl")
  doc <- xmlInternalTreeParse(tmp)
  unlink(tmp)
  
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
      day(dates) <- month_end(dates)
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
  
  na.omit(do.call(merge.xts, results), is.na="all")
}

#' Fetch data from World Bank
#' 
#' @param indicators a vector of World Bank indicators
#' @param countries a vector of countrie identifiers, which can be 2- or
#'   3-character ISO codes. The special option "all" retrieves all countries.
#' @return a xts object
#' @export
#' @examples
#' pdfetch_WB("NY.GDP.MKTP.CD", c("BR","MX"))
pdfetch_WB <- function(indicators, countries="all") {
  countries <- paste(countries, collapse=";")
  indicators <- paste(indicators, collapse=";")
  
  query <- paste0("http://api.worldbank.org/countries/",countries,"/indicators/",indicators,"?format=json&per_page=1000")
  x <- fromJSON(getURL(query))[[2]]
  
  if (!inherits(x, "data.frame")) {
    warning("No series found")
    return(NULL)
  }
  
  results <- data.frame(indicator=paste(x$indicator$id, x$country$id, sep="."),
                  value=as.numeric(x$value),
                  date=as.Date(ISOdate(as.numeric(x$date), 12, 31))) # This dating won't always work, need to detect frequency
  results <- dcast(results, date ~ indicator)
  results <- na.omit(xts(subset(results, select=-date), results$date), is.na="all")
  results
}

