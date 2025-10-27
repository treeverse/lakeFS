val nexus = "https://s01.oss.sonatype.org/"
publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}
// Remove all additional repository other than Maven Central from POM
pomIncludeRepository := { _ => false }
credentials ++= Seq(
  Credentials(Path.userHome / ".sbt" / "credentials"),
  Credentials(Path.userHome / ".sbt" / "sonatype_credentials")
)

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
