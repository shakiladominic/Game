package com.db.exercise

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class RDDRunner(val context: SparkContext) {
  /**
    * @param input a map of the file paths to extract. The key is a [[String]] alias, the value is the [[String]] path of the file to extract.
    * @return a map of [[RDD]] per each input
    */
    
  def extract(input: Map[String, String]): Map[String, RDD[String]] = {
    
    /* Load textfiles from local path, create RDDs and return as values to a new Map for use by next function(transform) */

     input.map{ case (key,value) => (key -> context.textFile(value))}  

  } 
  
  
  /**
    * @param extracted a map of [[RDD]] indexed by a [[String]] alias
    * @return
    */  
  
    def transform(extracted: Map[String, RDD[String]]): RDD[String] = {
      
      /* Split RDDs based on keys and process seperately as schema is different for both of the files. */

 
      val scoresrdd = extracted("SCORES")
      val teamsrdd = extracted("TEAMS")
      
     /* Below code reads RDD, splits lines to find player with top points. Player column is grouped by and sum of points is 
        calculated using reduceByKey function. Then, the RDD is sorted descending and first value is fetched to identify player with top
        points.
         
         Note: This code only finds the top player with maximum points.   */
      
      val scoresrdd_split = scoresrdd.map{ line =>
                           val fields = line.split(",")
                           val player = fields(0).toString                                          
                           val points = fields(2).toFloat
                           (player,points) }


    
      val scoresrdd_reduced = scoresrdd_split.reduceByKey((x,y) => x + y)
    
      val scoresrdd_flipped = scoresrdd_reduced.map( x => (x._2, x._1) )
      val top_player = scoresrdd_flipped.takeOrdered(1)(Ordering[(Float, String)].reverse)
      val scoresrdd_flipped_again = top_player.map(x =>(x._1, x._2) )
      val winner_player = "Top Player is" + scoresrdd_flipped_again(0)
   
    /* Below code reads RDD, splits lines to find team with top points. This RDD is joined with previous RDD which has points details.
       Team column is grouped by and sum of points is calculated using reduceByKey function. Then, the RDD is sorted descending 
       and first value is fetched to identify player with top points.
       
 
       Note: This code only finds the top team with maximum points.   */    
    
       val teamsrdd_split =  teamsrdd.map{ line =>
                           val fields = line.split(",")
                           val player = fields(0).toString                                          
                           val team = fields(1)
                           (player,team) }
 
        val teamsrdd_joined = scoresrdd_split.join(teamsrdd_split).map(x => (x._2._2,x._2._1))
        val teamsrdd_reduced = teamsrdd_joined.reduceByKey((x,y) => x + y)
    
        val teamsrdd_flipped = teamsrdd_reduced.map( x => (x._2, x._1) )
        val top_team = teamsrdd_flipped.takeOrdered(1)(Ordering[(Float, String)].reverse)
        val teamsrdd_flipped_again = top_team.map( x => (x._1, x._2) )
        val winner_team = "Top Team" + teamsrdd_flipped_again(0)
    
    
        /* Ordering method converts to Array[String,Float]. A new RDD is created with Arrays joined to return RDD[String]*/
        
        val rdd_Array = Array(winner_team,winner_player)
        context.parallelize(rdd_Array)
    
  }
    
  /**
    * @param transformed the [[RDD]] to store as a file
    * @param path the path to save the output file
    */
  def load(transformed: RDD[String], path: String): Unit = {
    
    /*coalesce is used to write output in one single file */
    
    transformed.coalesce(1)
    .saveAsTextFile(path)
  }

}

