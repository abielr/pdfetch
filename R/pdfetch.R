#' Fetch data from St Louis Fed's FRED database
#' 
#' @param identifiers a list of FRED series IDs
#' @param col.names an optional list of column names. If not supplied, the 
#' @return an xts object with all the series merged together
fetch_FRED <- function(identifiers, col.names) {
  results <- list()
  if (missing(col.names))
    col.names <- identifiers
  else if (length(identifiers) != length(col.names))
    stop("identifiers and col.names must be of same length")
  
  for (i in 1:length(identifiers)) {
    tmp <- tempfile()
    download.file(paste0("http://research.stlouisfed.org/fred2/series/",identifiers[i],"/downloaddata/",identifiers[i],".csv"), destfile=tmp, quiet=T) 
    fr <- read.csv(tmp,na.string=".")
    unlink(tmp)
    fr <- xts(as.matrix(fr[,2]), as.Date(fr[,1], origin="1970-01-01"))
    dim(fr) <- c(NROW(fr),1)
    colnames(fr) <- col.names[i]
    results[[identifiers[i]]] <- fr
  }
  
  do.call(merge.xts, results)
}

#' Fetch data from European Central Bank's statistical data warehouse
fetch_ECB <- function(identifiers, col.names) {
  results <- list()
  if (missing(col.names))
    col.names <- identifiers
  else if (length(identifiers) != length(col.names))
    stop("identifiers and col.names must be of same length")
  
  for (i in 1:length(identifiers)) {
    tmp <- tempfile()
    download.file(paste0("http://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=",identifiers[i],"&type=csv"), destfile=tmp, quiet=T) 
    fr <- read.csv(tmp, header=F, stringsAsFactors=F)[-c(1:5),]
    unlink(tmp)
    fr <- xts(as.matrix(as.numeric(fr[,2])), as.Date(fr[,1], origin="1970-01-01"))
    dim(fr) <- c(NROW(fr),1)
    colnames(fr) <- col.names[i]
    results[[identifiers[i]]] <- fr
  }
  
  do.call(merge.xts, results)
}

fetch_ECB("FM.B.U2.EUR.4F.KR.DFR.CHG")
