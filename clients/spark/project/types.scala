package build

class BuildType(
    val name: String,
    val sparkVersion: String,
    val scalapbVersion: String,
    val hadoopVersion: String,
    val hadoopFlavour: String, // If set, a directory of additional sources to compile
    val gcpConnectorVersion: String
)
