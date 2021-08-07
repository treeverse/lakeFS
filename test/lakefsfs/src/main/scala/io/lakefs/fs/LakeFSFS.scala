package io.lakefs.fs

import io.lakefs.clients.api.RepositoriesApi
import io.lakefs.clients.api.auth.HttpBasicAuth
import io.lakefs.clients.api.model.RepositoryCreation
import io.lakefs.clients.api.ApiException
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import org.slf4j.LoggerFactory

object LakeFSFS {
  private val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("LakeFSFS")
      .getOrCreate()

    val options = new Options()
      .addOption("r", "repository", true, "Repository name")
      .addOption("b", "branch", true, "Branch name")
      .addOption("s", "storage-namespace", true, "Storage namespace")
      .addOption("a", "amazon-reviews", true, "Amazon Customer Reviews dataset location")
    val parser = new BasicParser
    val cmd = parser.parse(options, args)
    val repository = cmd.getOptionValue("r", "example")
    val branch = cmd.getOptionValue("b", "main")
    val storageNamespace = cmd.getOptionValue("s", s"s3://$repository")
    val sourcePath = cmd.getOptionValue("a", "s3a://amazon-reviews-pds/parquet")

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    // use lakefs api client to create repository for this app
    val apiClient = makeApiClient(spark.conf)

    LOG.info("Create repository={}, branch={}, storageNamespace={}", repository, branch, storageNamespace)
    val repositoriesApi = new RepositoriesApi(apiClient)
    try {
      val repositoryCreation = new RepositoryCreation()
        .name(repository)
        .defaultBranch(branch)
        .storageNamespace(storageNamespace)
      repositoriesApi.createRepository(repositoryCreation, false)
      LOG.info("Repository created repository={}, branch={}, storageNamespace={}", repository, branch, storageNamespace)
    } catch {
      case e: ApiException => LOG.error("Create repository failed (code " + e.getCode() + ")", e)
    }

    LOG.info("Read data from {}", sourcePath)
    val pds = spark.read
      .parquet(sourcePath)
    pds.createOrReplaceTempView("pds")

    // select product id and title with number of reviews from 'Books' category and marketplace 'US'
    val productsReviewCount = spark.sql("SELECT product_id, product_title, year, COUNT(*) as num_reviews " +
      "FROM pds where product_category='Books' AND marketplace='US' " +
      "GROUP BY product_id, product_title, year")

    // write to lakeFS partition by 'year'
    val byYearPath = s"lakefs://$repository/$branch/amazon-reviews-pds/parquet"
    LOG.info("Write products by year to {}", byYearPath)
    productsReviewCount.write
      .partitionBy("year")
      .parquet(byYearPath)

    // read the data from lakeFS
    LOG.info("Read by year data and compute top 10 for 2015")
    val lfsPds = spark.read.parquet(byYearPath)
    lfsPds.createOrReplaceTempView("lfs_pds")
    val topTen = spark.sql("SELECT product_id, product_title, num_reviews " +
      "FROM lfs_pds where year=2015 " +
      "ORDER BY num_reviews DESC " +
      "LIMIT 10")
    topTen.collect.foreach(entry => {
      LOG.info("PRODUCT {}", entry)
    })

    spark.stop()
  }

  private def makeApiClient(conf: RuntimeConfig) = {
    // use lakefs hadoop fs configuration parameters
    LOG.info("Setup lakeFS API client")
    val accessKey = conf.get("spark.hadoop.fs.lakefs.access.key")
    val secretKey = conf.get("spark.hadoop.fs.lakefs.secret.key")
    val endpoint = conf.get("spark.hadoop.fs.lakefs.endpoint").stripSuffix("/")
    LOG.info("lakeFS API client endpoint={}", endpoint)
    val apiClient = io.lakefs.clients.api.Configuration.getDefaultApiClient
    apiClient.setBasePath(endpoint)

    val basicAuth = apiClient.getAuthentication("basic_auth").asInstanceOf[HttpBasicAuth]
    basicAuth.setUsername(accessKey)
    basicAuth.setPassword(secretKey)

    apiClient
  }
}
