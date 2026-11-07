package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogAnalysis {

    private static final Pattern LOG_PATTERN = Pattern.compile(
            "^(\\S+) \\S+ \\S+ \\[([^\\]]+)\\] \"(\\S+) (\\S+) ([^\"]+)\" (\\d{3}) (\\S+) \"([^\"]*)\" \"([^\"]*)\"$"
    );

    // Classe représentant une ligne du fichier Apache
    public static class LogEntry implements Serializable {
        String ip;
        String date;
        String method;
        String resource;
        int status;
        String responseSize;

        public LogEntry(String ip, String date, String method,
                        String resource, int status, String responseSize) {
            this.ip = ip;
            this.date = date;
            this.method = method;
            this.resource = resource;
            this.status = status;
            this.responseSize = responseSize;
        }

        @Override
        public String toString() {
            return "IP=" + ip
                    + " | Date=" + date
                    + " | Method=" + method
                    + " | Resource=" + resource
                    + " | Status=" + status
                    + " | Size=" + responseSize;
        }
    }

    // Extraction des informations depuis une ligne Apache
    private static LogEntry parseLog(String line) {
        // Supprimer un éventuel BOM UTF-8 au début du fichier
        String cleanLine = line.replace("\uFEFF", "").trim();

        Matcher matcher = LOG_PATTERN.matcher(cleanLine);

        if (matcher.matches()) {
            return new LogEntry(
                    matcher.group(1),
                    matcher.group(2),
                    matcher.group(3),
                    matcher.group(4),
                    Integer.parseInt(matcher.group(6)),
                    matcher.group(7)
            );
        }

        return null;
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Apache Log Analysis")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            // 1. Charger le fichier access.log
            JavaRDD<String> lines = sc.textFile("access.log");

            // 2. Parser les lignes et extraire les champs
            JavaRDD<LogEntry> logs = lines
                    .map(LogAnalysis::parseLog)
                    .filter(entry -> entry != null)
                    .cache();

            System.out.println("\n========== LOG RECORDS ==========");
            logs.collect().forEach(System.out::println);

            // 3. Statistiques générales
            long totalRequests = logs.count();

            long totalErrors = logs
                    .filter(log -> log.status >= 400)
                    .count();

            double errorPercentage = totalRequests == 0
                    ? 0.0
                    : (totalErrors * 100.0) / totalRequests;

            System.out.println("\n========== BASIC STATISTICS ==========");
            System.out.println("Total requests : " + totalRequests);
            System.out.println("Total HTTP errors : " + totalErrors);
            System.out.printf("Error percentage : %.2f%%%n", errorPercentage);

            // 4. Top 5 des adresses IP
            JavaPairRDD<String, Integer> ipCounts = logs
                    .mapToPair(log -> new Tuple2<>(log.ip, 1))
                    .reduceByKey(Integer::sum);

            List<Tuple2<Integer, String>> topIPs = ipCounts
                    .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                    .sortByKey(false)
                    .take(5);

            System.out.println("\n========== TOP 5 IP ADDRESSES ==========");
            topIPs.forEach(System.out::println);

            // 5. Top 5 des ressources demandées
            JavaPairRDD<String, Integer> resourceCounts = logs
                    .mapToPair(log -> new Tuple2<>(log.resource, 1))
                    .reduceByKey(Integer::sum);

            List<Tuple2<Integer, String>> topResources = resourceCounts
                    .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                    .sortByKey(false)
                    .take(5);

            System.out.println("\n========== TOP 5 REQUESTED RESOURCES ==========");
            topResources.forEach(System.out::println);

            // 6. Distribution des codes HTTP
            JavaPairRDD<Integer, Integer> statusCounts = logs
                    .mapToPair(log -> new Tuple2<>(log.status, 1))
                    .reduceByKey(Integer::sum);

            System.out.println("\n========== HTTP STATUS DISTRIBUTION ==========");
            statusCounts
                    .sortByKey()
                    .collect()
                    .forEach(System.out::println);

        } finally {
            sc.close();
        }
    }
}
