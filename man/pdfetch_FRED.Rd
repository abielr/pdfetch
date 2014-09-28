\name{pdfetch_FRED}
\alias{pdfetch_FRED}
\title{Fetch data from St Louis Fed's FRED database}
\usage{
pdfetch_FRED(identifiers)
}
\arguments{
  \item{identifiers}{a vector of FRED series IDs}
}
\value{
a xts object
}
\description{
Fetch data from St Louis Fed's FRED database
}
\examples{
tryCatch(pdfetch_FRED(c("GDPC1", "PCECC96")),
         error = function(e) {},
         warning = function(w) {}
)
}

