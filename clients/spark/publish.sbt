// Configure for Maven Central Portal using sbt 1.11+ built-in support
// sbt automatically reads SONATYPE_USERNAME and SONATYPE_PASSWORD env vars

// Use local staging for publishSigned, then sonaUpload to upload
publishTo := localStaging.value

// Remove all additional repository other than Maven Central from POM
pomIncludeRepository := { _ => false }

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/treeverse/lakeFS"),
    "scm:git@github.com:treeverse/lakeFS.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id = "ariels",
    name = "Ariel Shaqed (Scolnicov)",
    email = "ariels@treeverse.io",
    url = url("https://github.com/arielshaqed")
  ),
  Developer(
    id = "baraktr",
    name = "B. A.",
    email = "barak.amar@treeverse.io",
    url = url("https://github.com/nopcoder")
  ),
  Developer(
    id = "ozkatz",
    name = "Oz Katz",
    email = "oz.katz@treeverse.io",
    url = url("https://github.com/ozkatz")
  ),
  Developer(
    id = "johnnyaug",
    name = "J. A.",
    email = "yoni.augarten@treeverse.io",
    url = url("https://github.com/johnnyaug")
  ),
  Developer(
    id = "itai.admi",
    name = "Itai Admi",
    email = "itai.admi@treeverse.io",
    url = url("https://github.com/itaiad200")
  ),
  Developer(
    id = "niro",
    name = "Nir Ozery",
    email = "nir.ozery@treeverse.io",
    url = url("https://github.com/N-o-Z")
  )
)
