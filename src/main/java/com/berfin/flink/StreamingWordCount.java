
package com.berfin.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamingWordCount {

    public static void main(String[] args) throws Exception {

        // 1. Flink çalışma ortamını oluştur
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Socket üzerinden canlı veri al
        DataStream<String> lines =
                env.socketTextStream("localhost", 9999);

        // 3. Satırları kelimelere ayır ve her kelimeye 1 ata
        DataStream<Tuple2<String, Integer>> words =
                lines.flatMap(
                    new FlatMapFunction<String, Tuple2<String, Integer>>() {
                        @Override
                        public void flatMap(
                                String line,
                                Collector<Tuple2<String, Integer>> out) {

                            for (String word : line.toLowerCase().split("\\s+")) {
                                if (!word.isEmpty()) {
                                    out.collect(Tuple2.of(word, 1));
                                }
                            }
                        }
                    }
                );

        // 4. Aynı kelimeleri grupla ve sayılarını biriktir
        DataStream<Tuple2<String, Integer>> counts =
                words
                    .keyBy(value -> value.f0)
                    .sum(1);

        // 5. Sonuçları yazdır
        counts.print();

        // 6. Flink uygulamasını çalıştır
        env.execute("Berfin - Streaming WordCount");
    }
}
