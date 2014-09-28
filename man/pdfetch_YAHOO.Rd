\name{pdfetch_YAHOO}
\alias{pdfetch_YAHOO}
\title{Fetch data from Yahoo Finance}
\usage{
pdfetch_YAHOO(identifiers, fields = c("open", "high", "low", "close",
  "volume", "adjclose"), from = as.Date("2007-01-01"), to = Sys.Date())
}
\arguments{
  \item{identifiers}{a vector of Yahoo Finance tickers}

  \item{fields}{can be any of "open", "high", "low",
  "close", "volume", or "adjclose"}

  \item{from}{a Date object or string in YYYY-MM-DD format.
  If supplied, only data on or after this date will be
  returned}

  \item{to}{a Date object or string in YYYY-MM-DD format.
  If supplied, only data on or before this date will be
  returned}
}
\value{
a xts object
}
\description{
Fetch data from Yahoo Finance
}
\examples{
tryCatch({
   pdfetch_YAHOO(c("^gspc","^ixic"))
   pdfetch_YAHOO(c("^gspc","^ixic"), "adjclose")
   },
   error = function(e) {},
   warning = function(w) {}
)
}

