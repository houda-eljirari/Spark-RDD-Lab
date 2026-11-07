
package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TotalSalesByCityAndYear {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Total Sales By City And Year")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            // Charger les données
            JavaRDD<String> lines = sc.textFile("ventes.txt");

            // Extraire la ville, l'année et le prix
            JavaPairRDD<String, Double> sales = lines.mapToPair(line -> {
                String[] fields = line.trim().split("\\s+");

                String date = fields[0];
                String city = fields[1];
                String year = date.substring(0, 4);
                double price = Double.parseDouble(fields[3]);

                String key = city + " - " + year;

                return new Tuple2<>(key, price);
            });

            // Calculer le total par ville et par année
            JavaPairRDD<String, Double> totals =
                    sales.reduceByKey(Double::sum);

            // Afficher les résultats
            System.out.println("=== TOTAL DES VENTES PAR VILLE ET PAR ANNEE ===");

            for (Tuple2<String, Double> result : totals.collect()) {
                System.out.println(result._1() + " : " + result._2());
            }

        } finally {
            sc.close();
        }
    }
}
