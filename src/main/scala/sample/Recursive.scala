package sample
import org.apache.spark.sql.SparkSession
object Parent_child {
  def main(arg: Array[String]): Unit = {

    //Input file and out file path
    var input_file_path="Parent_child.csv"
    var output_file_path="vijay"


    val sparkSession: SparkSession = SparkSession.builder.master("local").appName("Scala Spark Example")
                .getOrCreate()
    val csvPO = sparkSession.read.option("inferSchema", true).option("header", true).
      csv(s"$input_file_path")

    csvPO.createOrReplaceTempView("main_data")

    //First One to hold value of number of rows in new data set

    var df_cnt: Int = 1

    //Second one to be used as counter
    var cnt: Int = 1

    //Create a dataframe Which will hold output of seed statement

    val df_ind_seed = sparkSession.sql("SELECT child_code,Parent_code,child_code hier_path " +
                                                  s",1 as level"+
                                                  " from main_data  where parent_code like 'root%'")

    //Register the Dataframe as temp table to be used in next step for iteration

    df_ind_seed.createOrReplaceTempView("vt_seed0");



    //Run the while loop to replicate iteration step

    while (df_cnt != 0) {
      var tblnm = "vt_seed".concat((cnt - 1).toString);
      var tblnm1 = "vt_seed".concat((cnt).toString);
      //println(tblnm)
      // sparkSession.sql(s"select * from $tblnm").show();
      val df_ind_rec = sparkSession.sql(s"select  subroot.child_code " +
                                              s",subroot.Parent_code  " +
                                              s",concat(subroot.child_code,'#',subroot.Parent_code) as hier_path"+
                                              s",root.level+1 level"+
                                              s" from $tblnm root INNER JOIN main_data subroot " +
                                              s"ON root.child_code=subroot.Parent_code"
      );
      //df_ind_rec.show()
      df_cnt = df_ind_rec.count().toInt;
      //println(df_cnt)
      if (df_cnt != 0) {
        df_ind_rec.createOrReplaceTempView(s"$tblnm1");
      }

      cnt = cnt + 1;
      //println(s"$tblnm1")

    }


    //sparkSession.sql(s"select  child_code,parent_code  from vt_seed0").show()

    var fin_query = "";

    for (a <- 0 to (cnt-2)) {
      if (a == 0) {
        fin_query = fin_query.concat("select child_code,Parent_code,hier_path,level from vt_seed")
          .concat(a.toString());

      }
      else {
        fin_query = fin_query.concat(" union select child_code,Parent_code,hier_path,level from vt_seed")
           .concat(a.toString());
      }
    }

    //println(fin_query)
    val result = sparkSession.sql(s"$fin_query");

    result.createOrReplaceTempView("final_tbl");

    val final_result=sparkSession.sql("SELECT a.*,CASE WHEN b.child_code is null then 0 else 1 end has_child" +
                                         " from final_tbl a " +
                                    "  LEFT JOIN final_tbl  b ON a.child_code=b.parent_code")

    final_result.show()
    //final_result.show();
    //Writing output data to CSV
    final_result.coalesce(1).write.option("header", "true")
                                               .mode("overwrite")
                                        .csv(s"$output_file_path")

  }
}