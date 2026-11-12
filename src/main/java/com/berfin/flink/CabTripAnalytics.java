
package com.berfin.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;

public class CabTripAnalytics {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        
        FileSource<String> source = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("/Volumes/KINGSTON/flink/flink-streaming-wordcount/cab-flink.txt")
                )
                .build();

        DataStream<String> lines = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Cab File Source"
        );

        DataStream<String[]> records = lines
                .map(line -> line.split(",", -1))
                .returns(Types.OBJECT_ARRAY(Types.STRING));

        DataStream<String[]> ongoing = records
                .filter(f -> f.length == 8
                        && f[4].trim().equalsIgnoreCase("yes"));

        DataStream<Tuple2<String, Integer>> destinations = ongoing
                .map(f -> Tuple2.of(
                        f[6].trim(),
                        Integer.parseInt(f[7].trim())
                ))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);

        destinations
                .map(value -> "DESTINATION: " + value.f0
                        + " | Passengers: " + value.f1)
                .print();

        DataStream<Tuple3<String, Integer, Integer>> pickup = ongoing
                .map(f -> Tuple3.of(
                        f[5].trim(),
                        Integer.parseInt(f[7].trim()),
                        1
                ))
                .returns(Types.TUPLE(
                        Types.STRING, Types.INT, Types.INT))
                .keyBy(value -> value.f0)
                .reduce((a, b) -> Tuple3.of(
                        a.f0, a.f1 + b.f1, a.f2 + b.f2
                ));

        pickup
                .map(v -> String.format(
                        "PICKUP: %s | Average: %.2f",
                        v.f0, (double) v.f1 / v.f2))
                .print();

        DataStream<Tuple3<String, Integer, Integer>> drivers = ongoing
                .map(f -> Tuple3.of(
                        f[3].trim(),
                        Integer.parseInt(f[7].trim()),
                        1
                ))
                .returns(Types.TUPLE(
                        Types.STRING, Types.INT, Types.INT))
                .keyBy(value -> value.f0)
                .reduce((a, b) -> Tuple3.of(
                        a.f0, a.f1 + b.f1, a.f2 + b.f2
                ));

        drivers
                .map(v -> String.format(
                        "DRIVER: %s | Average: %.2f",
                        v.f0, (double) v.f1 / v.f2))
                .print();

        env.execute("Assignment 1 - Cab Trip Analytics");
    }
}

