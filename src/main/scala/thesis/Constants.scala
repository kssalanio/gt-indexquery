package thesis

object Constants {
  val TILE_SIZE = 1024 //in pixels
//  val TILE_SIZE = 2048 //in pixels
  //val RDD_PARTS = 50
//  val RDD_PARTS = 4
  val RDD_PARTS = 40
//  val RUN_REPS = 5
  val RUN_REPS = 11
  val PAD_LENGTH = 8
  //val TESTS_CSV = "/home/spark/datasets/csv/test_params_looc.csv"
  //val TESTS_CSV = "/home/spark/datasets/csv/test_params_sablayan.csv"
  val TESTS_CSV = "/home/spark/datasets/csv/test_params_san_jose.csv"

  val SFC_LABEL_HILBERT="hilbert"
  val SFC_LABEL_ZORDER="zorder"
}
