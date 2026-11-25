plugins {
  id("strand.library-conventions")
}

description = "strand: 전송 독립 애플리케이션 API"

dependencies {
  testImplementation(libsCatalog.library("archunit"))
}
