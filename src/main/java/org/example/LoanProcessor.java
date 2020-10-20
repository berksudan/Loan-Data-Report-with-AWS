package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.log4j.Level;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.ml.feature.Bucketizer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class LoanProcessor {
    private final static String S3_BUCKET_PATH = "s3://loan-data-bucket-aws";
    private static final String ANNUAL_INCOME_COL = "annual_inc", lOAN_AMOUNT_COL = "loan_amnt", TERM_COL = "term";
    private static final String INCOME_RANGE_COL = "income range";
    private static final String FUNDED_COL = "funded_amnt", GRADE_COL = "grade", LOAN_STATUS_COL = "loan_status";


    public static void main(String[] args) throws URISyntaxException {
        final String BUCKET_PATH = "s3://loan-data-bucket-aws/";
        final String REPORT_ONE_OUTPUT_FILE = BUCKET_PATH + "report_one";
        final String REPORT_TWO_OUTPUT_FILE = BUCKET_PATH + "report_two";

        URI u = new URI(REPORT_ONE_OUTPUT_FILE);
        File filekk= (new File(u));
        System.out.println(filekk.exists());
        System.exit(174);
        // Start Spark Session.
        SparkSession spark = SparkSession.builder()
//                .master("local[*]")
                .appName("LoanProcessor")
                .getOrCreate();
        spark.sparkContext().setLogLevel(Level.ERROR.toString()); // FIXME: Can be enabled
        System.out.println("[INFO] Loan Processor is being started..");

        /*
        // Remove Output Files If Exist
        removeDirectoryIfExists(new File(REPORT_ONE_OUTPUT_FILE));
        removeDirectoryIfExists(new File(REPORT_TWO_OUTPUT_FILE));
         */
        String[] FileURIs = FileURIsRetriever.retrieve();
        String[] gzFiles = Arrays.stream(FileURIs).map(file -> S3_BUCKET_PATH + file).toArray(String[]::new);

        System.out.println("[INFO] GZIPed CSV Files:");
        Arrays.stream(gzFiles).forEach(System.out::println);

        Dataset<Row> loanDF = retrieveMultipleCSVsMerged(spark, gzFiles)
                .withColumn(ANNUAL_INCOME_COL, col(ANNUAL_INCOME_COL).cast(DataTypes.DoubleType)); // For consistency.

        System.out.println("[INFO] Total Data Count: " + loanDF.count());
        System.out.println("[INFO] Schema of Data:");
        loanDF.printSchema();
        System.out.println("[INFO] First Task Starting..");
        firstTask(loanDF, REPORT_ONE_OUTPUT_FILE);
        System.out.println("[INFO] Second Task Starting..");
        secondTask(loanDF, REPORT_TWO_OUTPUT_FILE);
        System.out.println("[INFO] Successfully Completed.");
    }

    public static String[] retrieveSortedContentFiles(String parentDir, String ext) {
        Collection<File> files = FileUtils.listFiles(
                new File(parentDir),
                new RegexFileFilter("^(.*?" + ext + ")"),
                DirectoryFileFilter.DIRECTORY);

        return files.stream() // Convert contentList to Stream in order to apply functions.
                .filter(file -> file.toString().endsWith(ext)) // Filter with file extension.
                .map(file -> Paths.get(file.toString()).toString()) // Prepend parent path.
                .sorted() // Sort file-names alphabetically.
                .toArray(String[]::new); // Convert to array of strings.
    }


    public static Dataset<Row> retrieveMultipleCSVsMerged(SparkSession spark, String[] files) {
        // Get first csv file in order to keep header.
        Dataset<Row> firstDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(files[0]);

        // Get other csv files.
        String[] filesWithoutFirst = Arrays.copyOfRange(files, 1, files.length);
        Dataset<Row> otherDFsMerged = spark.read()
                .schema(firstDF.schema())
                .csv(filesWithoutFirst);

        // Merge the first and the rest of the csv files.
        return firstDF.union(otherDFsMerged);
    }

    private static void firstTask(Dataset<Row> loanDF, String outputFile) {
        final double[] INCOME_INTERVAL_POINTS = {-1, 40000, 60000, 80000, 100000, Double.POSITIVE_INFINITY};
        final Map<String, String> BUCKET_NUMBERS_TO_NAMES = new HashMap<String, String>() {{
            put("0.0", "<40k");
            put("1.0", "40-60k");
            put("2.0", "60-80k");
            put("3.0", "80-100k");
            put("4.0", ">100k");
        }};

        // Select related columns to boost performance.
        Dataset<Row> df = loanDF.select(ANNUAL_INCOME_COL, lOAN_AMOUNT_COL, TERM_COL);
        // Drop null values.
        df = df.na().drop();

        // Split data into buckets.
        Bucketizer bucketizer = new Bucketizer()
                .setInputCol(ANNUAL_INCOME_COL)
                .setOutputCol(INCOME_RANGE_COL)
                .setSplits(INCOME_INTERVAL_POINTS);
        df = bucketizer.transform(df);

        // Convert "## Months" to "##" in TERM_COL.
        df = df.withColumn(TERM_COL, regexp_replace(col(TERM_COL), " 36 months", "36"))
                .withColumn(TERM_COL, regexp_replace(col(TERM_COL), " 60 months", "60"));

        // Group By INCOME_RANGE_COL and aggregate lOAN_AMOUNT_COL and INCOME_RANGE_COL
        df = df.groupBy(INCOME_RANGE_COL)
                .agg(avg(lOAN_AMOUNT_COL).as("avg amount"), avg(TERM_COL).as("avg term"))
                .sort(INCOME_RANGE_COL);

        // Change bucket numbers to categorical bucket names.
        for (Map.Entry<String, String> e : BUCKET_NUMBERS_TO_NAMES.entrySet())
            df = df.withColumn(INCOME_RANGE_COL, regexp_replace(col(INCOME_RANGE_COL), e.getKey(), e.getValue()));

        df.show();
        df.write().csv(outputFile);
    }

    private static void secondTask(Dataset<Row> loanDF, String outputFile) {
        final int LOAN_AMOUNT_COL_IDX = 0, FUNDED_COL_IDX = 1, LOAN_STATUS_COL_IDX = 3;

        // Select related columns to boost performance
        Dataset<Row> df = loanDF.select(lOAN_AMOUNT_COL, FUNDED_COL, GRADE_COL, LOAN_STATUS_COL);
        // Drop null values
        df = df.na().drop();

        df = df.filter((FilterFunction<Row>) row ->
                row.getDouble(LOAN_AMOUNT_COL_IDX) == row.getDouble(FUNDED_COL_IDX) &&
                        row.getDouble(LOAN_AMOUNT_COL_IDX) > 1000);

        Dataset<Row> gradeCountsDF = df.groupBy(GRADE_COL)
                .count()
                .withColumnRenamed("count", "cnt_grades");

        Dataset<Row> fpCountsDF = df.filter((FilterFunction<Row>) row -> row.getString(LOAN_STATUS_COL_IDX).equals("Fully Paid"))
                .groupBy(GRADE_COL)
                .count()
                .withColumnRenamed("count", "cnt_fp");

        Column fullyPaidAmountRateCol = concat(lit("%"),
                fpCountsDF.col("cnt_fp")
                        .divide(gradeCountsDF.col("cnt_grades"))
                        .multiply(100));
        df = gradeCountsDF.join(fpCountsDF, GRADE_COL) // Join 2 DFs over GRADE_COL.
                .withColumn("fully paid amount rate", fullyPaidAmountRateCol) // Add fullyPaidAmountRateCol.
                .drop("cnt_grades", "cnt_fp") // Drop redundant features.
                .sort(GRADE_COL); // Sort by grades.

        df.show();
        df.write().csv(outputFile);
    }

    /*
    public static void removeDirectoryIfExists(File dir) {
        if (!dir.exists()) {
            System.out.println("[INFO] " + dir.getPath() + " not exists.");
            return;
        }
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null && files.length > 0) {
                for (File aFile : files) {
                    removeDirectoryIfExists(aFile);
                }
            }
        }
        //noinspection ResultOfMethodCallIgnored
        dir.delete();
        System.out.println("[INFO] " + dir.getPath() + " deleted.");
    }
     */

}
