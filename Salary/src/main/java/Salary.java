
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.*;
import java.text.DateFormat;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.text.ParseException;
import java.lang.*;


import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import scala.Tuple6;


/**

	Category
	   - 17
	18 - 24
	25 - 39
	40 - 

	@author 
	@version 1.0
*/
public class Salary
{

	public static int year;

	/**
	Main method
	*/
	public static void main( String[] args )
	{
				
		Calendar cal = Calendar.getInstance();
		year = 1900 + cal.getTime().getYear();


		SparkConf conf = new SparkConf()
			.setAppName("Salary");
 
		JavaSparkContext sc = new JavaSparkContext( conf );

		JavaRDD<String> src = sc.textFile("/home/grey/hw2_kar/input.txt");

		JavaRDD<Tuple3<Integer,Integer,Integer>> src_tab = src.map( a -> StringToCols( a ) );

		JavaRDD<Tuple3<Integer,Integer,Integer>> src_tab_fil = src_tab.filter( a -> a._1() >= 0 );


		//
		JavaRDD<Tuple3<Integer,Integer,Integer>> sal_0 = src_tab_fil.filter( a -> a._2() == 0 );

		JavaPairRDD<Integer,Tuple2<Integer,Integer>> sal_1 = sal_0.mapToPair( a -> new Tuple2<>( a._1(), new Tuple2<>( a._3() ,1 ) ) );
		
		JavaPairRDD<Integer,Tuple2<Integer,Integer>> sal_2 = sal_1.reduceByKey( (a,b) -> new Tuple2<>( a._1()+b._1(), a._2()+b._2() ) );

		JavaPairRDD<Integer,Double> sal_3 = sal_2.mapToPair(
			a -> new Tuple2<>( a._1(), a._2()._1().doubleValue() / a._2()._2().doubleValue() ) );

		sal_3.saveAsTextFile("/home/grey/hw2_kar/output_salary");		
		//

		//
		JavaRDD<Tuple3<Integer,Integer,Integer>> abr_0 = src_tab_fil.filter( a -> a._2() == 1 );

		JavaPairRDD<Integer,Tuple2<Integer,Integer>> abr_1 = abr_0.mapToPair( a -> new Tuple2<>( a._1(), new Tuple2<>( a._3() ,1 ) ) );
		
		JavaPairRDD<Integer,Tuple2<Integer,Integer>> abr_2 = abr_1.reduceByKey( (a,b) -> new Tuple2<>( a._1()+b._1(), a._2()+b._2() ) );

		JavaPairRDD<Integer,Double> abr_3 = abr_2.mapToPair(
			a -> new Tuple2<>( a._1(), a._2()._1().doubleValue() / a._2()._2().doubleValue() ) );

		abr_3.saveAsTextFile("/home/grey/hw2_kar/output_abroad");			
		//
		
		
		//src_tab.saveAsTextFile("/home/grey/hw2_kar/output_abroad");


		/*JavaPairRDD<MsgKey, Integer> reduced = CalcRDD( raw_log );

		JavaRDD<Tuple6<Integer,String,Long,LocalDate,Integer,Integer>> result =
			reduced.map( a -> new Tuple6<>( 
				a._1().getTime(), a._1().getTimeString(), a._1().getTimeLong(), a._1().getDate(), a._1().getPrior(), a._2() ) );

		javaFunctions( result )
			.writerBuilder( "syslog_ks", "syslog_stat", mapTupleToRow( Integer.class, String.class, Long.class, LocalDate.class, Integer.class, Integer.class ) )
			.withColumnSelector( someColumns( "timev", "times", "timet", "datet", "prior", "count" ) )
			.saveToCassandra();*/

		sc.stop();
	}


	public static Tuple3<Integer,Integer,Integer> StringToCols( String a )
	{

		int age = -1;
		int type = 0;
		int value = 0;

		String params[] = a.split(" ");

		if( params.length != 4 )
		{
			return new Tuple3<>( new Integer(age), new Integer(type), new Integer(value) );
		}

		try
		{
			age = Integer.parseInt( params[1] );
		}catch(NumberFormatException e)
		{ age=-1; return new Tuple3<>( new Integer(age), new Integer(type), new Integer(value) );}

		try
		{
			type = Integer.parseInt( params[2] );
		}catch(NumberFormatException e)
		{ age=-1; return new Tuple3<>( new Integer(age), new Integer(type), new Integer(value) );}

		try
		{
			value = Integer.parseInt( params[3] );
		}catch(NumberFormatException e)
		{ age=-1; return new Tuple3<>( new Integer(age), new Integer(type), new Integer(value) );}

		Calendar cal = Calendar.getInstance();
		age = year - age;
		
		if( age < 0 )
			age = -1;
		else if( age < 18 )
			age = 0;
		else if( age < 25 )
			age = 1;
		else if( age < 40 )
			age = 2;
		else
			age = 3;
		
		return new Tuple3<>( new Integer(age), new Integer(type), new Integer(value) );
	}

	/**
	@param - raw_log input RDD of string
	@return - reduced results
	*/
	/*public static JavaPairRDD<MsgKey, Integer> CalcRDD( JavaRDD<String> raw_log )
	{
		JavaPairRDD<MsgKey, Integer> messages = raw_log.mapToPair( a -> new Tuple2<>( new MsgKey(a), 1 ) );

		JavaPairRDD<MsgKey, Integer> messages_fil = messages.filter( a -> a._1().getTime() > 0 );

		JavaPairRDD<MsgKey, Integer> reduced = messages_fil.reduceByKey( (a,b) -> a+b );

		return reduced;
	}*/

}














