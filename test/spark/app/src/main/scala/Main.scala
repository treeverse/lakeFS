object Main {
  def main(args: Array[String]): Unit = {
    Sonnets.testSonnets(args)
    if (args.size > 2) {
      Console.println(s"Test multipart upload into ${args(2)}")
      Multipart.testMultipart(args(2))
    }
  }
}

