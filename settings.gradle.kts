pluginManagement {
  includeBuild("build-logic")
}

plugins {
  id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "strand"

include("strand-core")
include("strand-nio")
include("strand-conformance")
include("strand-examples")
include("strand-integration-test")
include("strand-benchmark")
