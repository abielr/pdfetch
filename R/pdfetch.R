#' Fetch data from St Louis Fed's FRED database
#' 
#' @param identifiers a list of FRED series IDs
#' @param col.names an optional list of column names. If not supplied, the 
#' @return an xts object with all the series merged together
pdfetch_FRED <- function(identifiers, col.names) {
  if (missing(col.names))
    col.names <- identifiers
  else if (length(identifiers) != length(col.names))
    stop("identifiers and col.names must be of same length")
  
  results <- list()
  for (i in 1:length(identifiers)) {
    tmp <- tempfile()
    download.file(paste0("http://research.stlouisfed.org/fred2/series/",identifiers[i],"/downloaddata/",identifiers[i],".csv"), destfile=tmp, quiet=T) 
    fr <- read.csv(tmp,na.string=".")
    unlink(tmp)
    x <- xts(as.matrix(fr[,2]), as.Date(fr[,1], origin="1970-01-01"))
    dim(x) <- c(NROW(x),1)
    colnames(x) <- col.names[i]
    results[[identifiers[i]]] <- x
  }
  
  do.call(merge.xts, results)
}

#' Fetch data from European Central Bank's statistical data warehouse
pdfetch_ECB <- function(identifiers, col.names) {
  if (missing(col.names))
    col.names <- identifiers
  else if (length(identifiers) != length(col.names))
    stop("identifiers and col.names must be of same length")
  
  results <- list()
  for (i in 1:length(identifiers)) {
    tmp <- tempfile()
    download.file(paste0("http://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=",identifiers[i],"&type=csv"), destfile=tmp, quiet=T) 
    fr <- read.csv(tmp, header=F, stringsAsFactors=F)[-c(1:5),]
    unlink(tmp)
    x <- xts(as.matrix(as.numeric(fr[,2])), as.Date(fr[,1], origin="1970-01-01"))
    dim(x) <- c(NROW(x),1)
    colnames(x) <- col.names[i]
    results[[identifiers[i]]] <- x
  }
  
  do.call(merge.xts, results)
}

#' Fetch data from Eurostat
pdfetch_EUROSTAT <- function(flowRef, key, startPeriod, endPeriod, col.names) {
  if (!missing(startPeriod) && !missing(endPeriod))
    url <- paste0("http://ec.europa.eu/eurostat/SDMX/diss-web/rest/data/",flowRef,"/",key,"/?startPeriod=",startPeriod,"&endPeriod=",endPeriod)
  else if (!missing(startPeriod))
    url <- paste0("http://ec.europa.eu/eurostat/SDMX/diss-web/rest/data/",flowRef,"/",key,"/?startPeriod=",startPeriod)
  else if (!missing(endPeriod))
    url <- paste0("http://ec.europa.eu/eurostat/SDMX/diss-web/rest/data/",flowRef,"/",key,"/?endPeriod=",endPeriod)
  else
    url <- paste0("http://ec.europa.eu/eurostat/SDMX/diss-web/rest/data/",flowRef,"/",key)
  
  tmp <- tempfile()
  download.file(url, destfile=tmp, quiet=T, method="curl")
  doc <- xmlInternalTreeParse(tmp)
  unlink(tmp)
  
  results <- list()
  seriesSet <- getNodeSet(doc, "//generic:Series")
  
  if (!missing(col.names) && length(seriesSet) != length(col.names))
    stop(paste("col.names of wrong length: should have", length(seriesSet), "elements"))
    
  for (i in 1:length(seriesSet)) {
    series <- seriesSet[[i]]
    id <- paste0(getNodeSet(series, "generic:SeriesKey/generic:Value/@value", "generic"), collapse=".")
    freq <- xmlGetAttr(getNodeSet(series, "generic:SeriesKey/generic:Value[@id='FREQ']", "generic")[[1]], "value")
    dates <- as.Date(as.yearmon(unlist(getNodeSet(series, "//generic:ObsDimension/@value", "generic")), format="%Y-%m"))
    day(dates) <- days_in_month(dates)
    values <- as.numeric(getNodeSet(series, "//generic:ObsValue/@value", "generic"))
    
    x <- xts(values, dates)
    dim(x) <- c(NROW(x),1)
    if (!missing(col.names))
      colnames(x) <- col.names[i]
    else
      colnames(x) <- id
    results[[i]] <- x
  }
  
  do.call(merge.xts, results)
}

#' Fetch data from World Bank
pdfetch_WB <- function(indicators, countries="all", col.names) {
  countries <- paste(countries, collapse=";")
  indicators <- paste(indicators, collapse=";")
  
  query <- paste0("http://api.worldbank.org/countries/",countries,"/indicators/",indicators,"?format=json&per_page=1000")
  x <- fromJSON(getURL(query))[[2]]
  results <- data.frame(indicator=paste(x$indicator$id, x$country$id, sep="."),
                  value=as.numeric(x$value),
                  date=as.Date(ISOdate(as.numeric(x$date), 12, 31))) # This dating won't always work, need to detect frequency
  results <- dcast(results, date ~ indicator)
  results <- xts(subset(results, select=-date), results$date)
  results
}

x <- pdfetch_WB("NY.GDP.MKTP.CD", c("br","ca"))

getURL("http://data.ons.gov.uk/ons/api/data/concepts.xml?apikey=shufTniwRu&context=Census")
x <-fromJSON(getURL("http://api.worldbank.org/countries/br/indicators/NY.GDP.MKTP.CD?format=json&per_page=1000"))[[2]]

x <- data.frame(a=1:3, b=c(2,3,NA))
