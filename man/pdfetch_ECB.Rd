\name{pdfetch_ECB}
\alias{pdfetch_ECB}
\title{Fetch data from European Central Bank's statistical data warehouse}
\usage{
pdfetch_ECB(identifiers)
}
\arguments{
  \item{identifiers}{a vector of ECB series IDs}
}
\value{
a xts object
}
\description{
Fetch data from European Central Bank's statistical data
warehouse
}
\examples{
tryCatch(pdfetch_ECB("FM.B.U2.EUR.4F.KR.DFR.CHG"),
         error = function(e) {},
         warning = function(w) {}
)
}

