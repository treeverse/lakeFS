package io.treeverse.gc

import io.treeverse.clients.SparkSessionSetup
import org.scalatest.funspec._
import org.scalatest.matchers._
import org.scalatestplus.mockito.MockitoSugar

class CommittedAddressListerSpec
    extends AnyFunSpec
    with SparkSessionSetup
    with should.Matchers
    with MockitoSugar {

  describe("naive lister") {

    it("should filter managed addresses") {
      withSparkSession(spark => {
        val cols =
          Seq("key", "address", "etag", "last_modified", "size", "range_id", "address_type")
        val data = Seq(
          ("committed_undeleted.json",
           "data/gqv3epl4fd2o8m3men70/ce8co454fd2o8m3menag",
           "e1ed577998f2d4425ccb1f3d014a6ca3",
           "2022-12-07 19:23:29",
           "130",
           "1b52584fe5a032e0fbc8fdfdf4776a02f142d68a37dacdfc4b6b8da5a6837d03",
           "RELATIVE"
          ),
          ("link_undeleted.json",
           "s3://test-bucket/lfs/data/gqv3epl4fd2o8m3men70/ce8cob54fd2o8m3menbg",
           "e1ed577998f2d4425ccb1f3d014a6ca3",
           "2022-12-07 19:23:57",
           "130",
           "1b52584fe5a032e0fbc8fdfdf4776a02f142d68a37dacdfc4b6b8da5a6837d03",
           "FULL"
          ),
          ("committed_undeleted.json",
           "d20cfd37cd0a4dcc80e3ce95446d11eb",
           "e1ed577998f2d4425ccb1f3d014a6ca3",
           "2022-12-07 19:09:05",
           "130",
           "518d57d4681e836c89f798ef0ad35eb305c696b01a421937b9656a1a4b87980d",
           "RELATIVE"
          ),
          ("not_deleted/0.json",
           "s3://some-test/0.json",
           "33adccd4f15155fb872106eb8743601a",
           "2022-10-09 10:52:56",
           "424",
           "85a9df08615335e6b844f20126b6807ffa8d4a4eb2d1c95cec1a5766b619896c",
           "FULL"
          ),
          ("committed_deleted.json",
           "5078f67ece2d495fbe056d5d6bdc93a2",
           "e1ed577998f2d4425ccb1f3d014a6ca3",
           "2022-12-07 14:25:45",
           "130",
           "a03dcbc7966a509f4a016f278862e47287a72c95a63e959c9affeed020c924f2",
           "BY_PREFIX_DEPRECATED"
          ),
          ("committed_undeleted.json",
           "cf4abf678fe54bfebd342a4d93ff05a0",
           "e1ed577998f2d4425ccb1f3d014a6ca3",
           "2022-12-07 14:25:36",
           "130",
           "a03dcbc7966a509f4a016f278862e47287a72c95a63e959c9affeed020c924f2",
           "BY_PREFIX_DEPRECATED"
          ),
          ("committed_deleted.json",
           "5b72f534f8014e55af18bd59b9235a27",
           "e1ed577998f2d4425ccb1f3d014a6ca3",
           "2022-12-07 16:57:41",
           "130",
           "b28f8bf00ae8b8d38e691c71c704dd26343d7760fd732c090b59bc7d4b735120",
           "RELATIVE"
          ),
          ("committed_undeleted.json",
           "482aaffe969747b7a83c2d08816c5c22",
           "e1ed577998f2d4425ccb1f3d014a6ca3",
           "2022-12-07 16:57:32",
           "130",
           "b28f8bf00ae8b8d38e691c71c704dd26343d7760fd732c090b59bc7d4b735120",
           "RELATIVE"
          ),
          ("link_undeleted.json",
           "s3://test-bucket/lfs/data/gc-migrate-lakefs-0-40-1:b40b9540-91f0-4add-96ba-205f17e03c57/77I_VsbUg1vU05hWRnTlw",
           "e1ed577998f2d4425ccb1f3d014a6ca3",
           "2022-12-07 17:59:14",
           "130",
           "b28f8bf00ae8b8d38e691c71c704dd26343d7760fd732c090b59bc7d4b735120",
           "FULL"
          ),
          ("committed_deleted.json",
           "data/gqv3epl4fd2o8m3men70/ce8co6d4fd2o8m3menb0",
           "e1ed577998f2d4425ccb1f3d014a6ca3",
           "2022-12-07 19:23:38",
           "130",
           "cc8a36bca7eda591d36cdb03b06a27721efd25f3ceb3b8fa20125a95a519606d",
           "RELATIVE"
          ),
          ("committed_undeleted.json",
           "data/gqv3epl4fd2o8m3men70/ce8co454fd2o8m3menag",
           "e1ed577998f2d4425ccb1f3d014a6ca3",
           "2022-12-07 19:23:29",
           "130",
           "cc8a36bca7eda591d36cdb03b06a27721efd25f3ceb3b8fa20125a95a519606d",
           "RELATIVE"
          ),
          ("link_undeleted.json",
           "s3://test-bucket/lfs/data/gqv3epl4fd2o8m3men70/ce8cob54fd2o8m3menbg",
           "e1ed577998f2d4425ccb1f3d014a6ca3",
           "2022-12-07 19:23:57",
           "130",
           "cc8a36bca7eda591d36cdb03b06a27721efd25f3ceb3b8fa20125a95a519606d",
           "FULL"
          )
        )
        val df = spark.createDataFrame(data).toDF(cols: _*)
        val addressLister = new NaiveCommittedAddressLister()
        val storageNamespace = "s3://test-bucket/lfs/"
        val addresses =
          addressLister.filterAddresses(spark, df, storageNamespace)

        val expected = Set(
          "data/gc-migrate-lakefs-0-40-1:b40b9540-91f0-4add-96ba-205f17e03c57/77I_VsbUg1vU05hWRnTlw",
          "data/gqv3epl4fd2o8m3men70/ce8co454fd2o8m3menag",
          "5078f67ece2d495fbe056d5d6bdc93a2",
          "cf4abf678fe54bfebd342a4d93ff05a0",
          "5b72f534f8014e55af18bd59b9235a27",
          "data/gqv3epl4fd2o8m3men70/ce8cob54fd2o8m3menbg",
          "d20cfd37cd0a4dcc80e3ce95446d11eb",
          "482aaffe969747b7a83c2d08816c5c22",
          "data/gqv3epl4fd2o8m3men70/ce8co6d4fd2o8m3menb0"
        )
        import spark.implicits._
        addresses.map(x => x.getString(0)).collect().toSet should be(expected)
      })
    }

  }
}
