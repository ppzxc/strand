plugins {
  id("strand.benchmark-conventions")
}

dependencies {
  jmh(project(":strand-core"))
}
