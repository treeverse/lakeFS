package build

class BuildType(
    val suffix: String,
    val sparkVersion: String,
    val scalapbVersion: String,
    val hadoopFlavour: String, // If set, a directory of additional sources to compile
    val gcpConnectorVersion: String
)
