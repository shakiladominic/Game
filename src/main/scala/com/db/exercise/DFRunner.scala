package com.db.exercise


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.desc



class DFRunner(val spark: SparkSession) {
 
  /**
    * @param input a map of the file paths to extract. The key is a [[String]] alias, the value is the [[String]] path of the file to extract.
    * @return a map of [[DataFrame]] per each input
    */
  def extract(input: Map[String, String]): Map[String, DataFrame] = {
    /* Load textfiles from local path, create Dataframes and return as values to a new Map for use by next function(transform) */
    
    input.map{ case (key,value) => (key -> spark.read.csv(value))}
    
  }

  /**
    * @param extracted a map of [[DataFrame]] indexed by a [[String]] alias
    * @return
    */
    def transform(extracted: Map[String, DataFrame]): DataFrame = {
      
      /* Split Dataframes based on keys and process seperately as schema is different for both of the files. */
  
      val scoresdf = extracted("SCORES")
      val teamsdf = extracted("TEAMS")
      
      /* Below code creates column names for Dataframes to avoid ambiguity, cast string to Float for calculating points, agrregate 
         points by Player and find the top player.
         
         Note: This code only finds the top player with maximum points.   */
      
      import spark.implicits._
      val ScoresSchema = Seq("Player", "Day", "Points")
      val ScoresRenamed = scoresdf.toDF(ScoresSchema: _*)
   
      val Scorescasted = ScoresRenamed.selectExpr("cast(Points as Float) as Points","Player","Day")
      val Scoresagg = Scorescasted.groupBy("Player").sum("Points")

      val Top_player = (Scoresagg.sort(desc("sum(Points)")).limit(1))
    
      /* Below code creates column names for Dataframes to avoid ambiguity, join teamsdataframe with previous dataframe, aggregate 
         points by Team and find the top team.
         
         Note: This code only finds the top player with maximum points.   */   
    
      val TeamsSchema = Seq("Player", "Team")
      val TeamsRenamed = teamsdf.toDF(TeamsSchema: _*)
      val Teamsjoined = Scorescasted.join(TeamsRenamed,"Player")
    
   
      val Teamsagg = Teamsjoined.groupBy("Team").sum("Points")
      
      val Top_team =  (Teamsagg.sort(desc("sum(Points)")).limit(1))
    
      /* Join both the dataframes to return a single dataframe to next function(load) */
      Top_player.union(Top_team)

  }

  /**
    * @param transformed the [[DataFrame]] to store as a file
    * @param path the path to save the output file
    */
  def load(transformed: DataFrame, path: String): Unit = {
    
    /*coalesce is used to write output in one single file */
    
    transformed.coalesce(1)
    .write
    .csv(path)
  }
}
