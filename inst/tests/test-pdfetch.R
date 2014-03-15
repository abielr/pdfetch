test_that("test Yahoo", {
  x <- pdfetch_YAHOO(c("^gspc","^ixic"), "adjclose")
  x <- pdfetch_YAHOO(c("^gspc","^ixic"))
  x <- pdfetch_YAHOO(c("vwo"))
})

test_that("test FRED", {
  x <- pdfetch_FRED(c("MDOTHFRAFDICTP1T4FR","UNRATE","GDPCA"))
})

test_that("test ECB", {
  x <- pdfetch_ECB("FM.B.U2.EUR.4F.KR.DFR.CHG")
  x <- pdfetch_ECB("IEAQ.Q.I6.N.V.B10.Z.S1M.A1.S.1.X.E.Z")
  x <- pdfetch_ECB("EXR.H.AUD.EUR.SP00.A")
  x <- pdfetch_ECB(c("EXR.D.E1.EUR.EN00.A", "IEAQ.Q.I6.N.V.B10.Z.S1M.A1.S.1.X.E.Z"))
})

test_that("test Eurostat", {
  pdfetch_EUROSTAT_DSD("namq_gdp_c")
  pdfetch_EUROSTAT_DSD("cdh_e_fos")
  pdfetch_EUROSTAT_DSD("irt_euryld_d")
  x <- pdfetch_EUROSTAT("cdh_e_fos", FREQ="A", Y_GRAD="TOTAL", FOS07=c("FOS1","FOS2"))
  x <- pdfetch_EUROSTAT("namq_gdp_c", FREQ="Q", S_ADJ="SWDA", UNIT="MIO_EUR", INDIC_NA="B1GM", GEO=c("DE","UK"))
  x <- pdfetch_EUROSTAT("irt_euryld_d", startPeriod=as.Date("2014-01-15"), MATURITY="Y1", FREQ="D", CURV_TYP="YCSR_RT")

})

test_that("test World Bank", {
  x <- pdfetch_WB("NY.GDP.MKTP.CD", c("BR","MX"))
})

test_that("test Bank of England", {
  x <- pdfetch_BOE(c("LPMVWYR", "LPMVWYR"), "2012-01-01")
})