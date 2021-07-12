package minsait.ttaa.datio.engine;

import minsait.ttaa.datio.common.PropertiesLoad;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;
import org.xml.sax.InputSource;
import scala.xml.Source;

import java.util.Properties;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;
    final static Properties p = PropertiesLoad.getPropertiesFile();

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df.printSchema();

        df = cleanData(df);
        df = exampleWindowFunction(df);
        df = rankOverWindow(df);
        df = getPotentialVsOverall(df);
        df = filteringByPlayerCatAndPotOverall(df);
        df = columnSelection(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);
        df.printSchema();

        // Uncomment when you want write your final output
        write(df);
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                catHeightByPosition.column(),
                playerCat.column(),
                potentialVsOverall.column()
        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {

        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(p.getProperty(INPUT_PATH));
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> exampleWindowFunction
    (Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(50), "B")
                .otherwise("C");

        df = df.withColumn(catHeightByPosition.getName(), rule);

        return df;
    }




    /**
     * @param df is a Dataset with players information (must have nationality and overall columns)
     * @return add to the Dataset the column "player_cat"
     * A si el jugador es de los mejores 10 jugadores de su país.
     * B si el jugador es de los mejores 20 jugadores de su país.
     * C si el jugador es de los mejores 50 jugadores de su país.
     * D para el resto de jugadores.
     */
    private Dataset<Row> rankOverWindow (Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column())
                .orderBy(overall.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(20), "B")
                .when(rank.$less(50), "C")
                .otherwise("D");

        df = df.withColumn(playerCat.getName(), rule);

        return df;
    }


    /**
     * @param df is a Dataset with players information (must have potential and overall columns)
     * @return add to the Dataset the column "potential_vd_overall"
     * potential divided by overall
     */
    private Dataset<Row> getPotentialVsOverall (Dataset<Row> df) {

        df = df.withColumn(potentialVsOverall.getName(),
                round(potential.column().divide(overall.column()),2));

        return df;
    }



    /**
     * @param df is a Dataset with players information (must have player_cat and potential_vs_overall columns)
     * @return add to the Dataset filtering by the next rules:
     * Si player_cat esta en los siguientes valores: A, B
     * Si player_cat es C y potential_vs_overall es superior a 1.15
     * Si player_cat es D y potential_vs_overall es superior a 1.25
     */
    private Dataset<Row> filteringByPlayerCatAndPotOverall (Dataset<Row> df) {

        df = df.filter(
                playerCat.column().equalTo("A").or(playerCat.column().equalTo("B")).or(
                        playerCat.column().equalTo("C").and(potentialVsOverall.column().$greater(1.15))
                        ).or(
                        playerCat.column().equalTo("D").and(potentialVsOverall.column().$greater(1.25))
                        )
        );

        return df;
    }

}
