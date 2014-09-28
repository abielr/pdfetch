\name{pdfetch_BOE}
\alias{pdfetch_BOE}
\title{Fetch data from the Bank of England Interactive Statistical Database}
\usage{
pdfetch_BOE(identifiers, from, to = Sys.Date())
}
\arguments{
  \item{identifiers}{a vector of BoE series codes}

  \item{from}{start date}

  \item{to}{end date; if not given, today's date will be
  used}
}
\value{
a xts object
}
\description{
Fetch data from the Bank of England Interactive Statistical
Database
}
\examples{
tryCatch(pdfetch_BOE(c("LPMVWYR", "LPMVWYR"), "2012-01-01"),
         error = function(e) {},
         warning = function(w) {}
)
}

