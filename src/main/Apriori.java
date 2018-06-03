/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package main;

/**
 *
 * @author cansu
 */

import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;
import scala.Tuple2;

public class Apriori {
	
    //static int ind = 2;

    public Apriori() {

    }

    //private static String appName = "Apriori_Example";

    //public static final long serialVersionUID = 1L;
    public JavaRDD<AssociationRules.Rule<String>> run(JavaSparkContext sc, double sup, String path, double conf) {

            JavaRDD<String> rddX = sc.textFile(path);
            Double count = rddX.count() * sup;
            Long minSupport = count.longValue();		
            JavaRDD<List<String>> rddx = rddX.map(m -> Arrays.asList(m.split(" ")));
            JavaRDD<String> flat = rddx.flatMap(f -> f.iterator());		
            JavaPairRDD<String, Integer> flatmap = flat.mapToPair(s -> new Tuple2<>(s, 1));
            flatmap = flatmap.reduceByKey((x, y) -> x + y);
            flatmap = flatmap.filter(s ->s._2 >=minSupport);	

            JavaPairRDD<List<String>, Integer> pair = flatmap.mapToPair(x-> new Tuple2<>(Arrays.asList(x._1), x._2));		
            JavaRDD<List<String>> rddx0 = pair.map(x-> x._1);
            int indis =2;			
            while(rddx0.cache().isEmpty()==false) {		
                    JavaPairRDD<List<String>, List<String>> cart = rddx0.cartesian(rddx0);
                    cart = cart.coalesce(6);
                    JavaRDD<List<String>> cart2 = cart.map(x->{
                            List<String> tmp = new ArrayList<>(x._1.size()+x._2.size());
                            tmp.addAll(x._1);
                            tmp.addAll(x._2);				
                            tmp = tmp.stream().distinct().collect(Collectors.toList()); //ayný geçen deðerleri sildi
                            Collections.sort(tmp);
                            return tmp;
                    });

                    int ind = indis ;
                    cart2 = cart2.filter(x -> x.size() == ind);
                    cart2 = cart2.distinct();			
                    JavaPairRDD<List<String>, List<String>> newcart = cart2.cartesian(rddx);
                    newcart = newcart.coalesce(6);
                    newcart = newcart.filter(x-> x._2.containsAll(x._1));
                    JavaPairRDD<List<String>, Integer> newmap = newcart.mapToPair(x-> new Tuple2<>(x._1, 1));
                    newmap = newmap.reduceByKey((x,y)-> x+y);
                    newmap = newmap.filter(x-> x._2>=minSupport);			
                    pair = pair.union(newmap);			
                    rddx0 = newmap.map(x-> x._1);	
                    indis ++;
            }
            JavaRDD<FPGrowth.FreqItemset<String>> freqItemsets = pair.map(x-> new FreqItemset<String>(x._1.toArray(), x._2));		  
            AssociationRules arules = new AssociationRules().setMinConfidence(conf);
            JavaRDD<AssociationRules.Rule<String>> results = arules.run(freqItemsets);

            //System.out.println(pair.collect());
            /*
            for (AssociationRules.Rule<String> rule : results.collect()) {
                    System.out.println(rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
            }*/		
            return results;
    }
	
}

