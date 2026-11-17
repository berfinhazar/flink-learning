package com.berfin.flink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/** Small bounded replay dataset, analyzed using Flink keyed state. */
public class TwitterDataAnalysis {
    public static void main(String[] args) throws Exception {
        Path input = Path.of(args.length > 0 ? args[0] : "data/tweets-sample.jsonl");
        Path output = Path.of(args.length > 1 ? args[1] : "output/counts.csv");
        String zone = args.length > 2 ? args[2] : "UTC";
        ZoneId.of(zone);
        List<String> lines = Files.readAllLines(input).stream().filter(s -> !s.isBlank()).toList();
        if (lines.isEmpty()) throw new IllegalArgumentException("Empty input: " + input);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        var counts = env.fromData(lines.toArray(String[]::new))
            .flatMap(new ExtractRelevantTweet(zone))
            .keyBy(t -> Tuple2.of(t.f0, t.f1),
                org.apache.flink.api.common.typeinfo.Types.TUPLE(
                    org.apache.flink.api.common.typeinfo.Types.STRING,
                    org.apache.flink.api.common.typeinfo.Types.STRING)).sum(2);
        Files.createDirectories(output.toAbsolutePath().getParent());
        try (CloseableIterator<Tuple3<String, String, Integer>> results = counts.executeAndCollect();
             var writer = Files.newBufferedWriter(output)) {
            writer.write("device,hour,count\n");
            while (results.hasNext()) {
                var row = results.next();
                String csv = row.f0 + "," + row.f1 + "," + row.f2;
                writer.write(csv + "\n");
                System.out.println(csv);
            }
        }
        System.out.println("Output: " + output.toAbsolutePath());
    }

    public static class ExtractRelevantTweet implements FlatMapFunction<String, Tuple3<String, String, Integer>> {
        private final String zone;
        private static final List<String> KEYWORDS = List.of("global warming", "pollution", "save earth",
            "temperature increase", "weather change", "climate", "co2", "air quality", "dust",
            "carbondioxide", "greenhouse", "ozone", "methane", "sealevel", "sea level");
        private static final DateTimeFormatter TWITTER_TIME =
            DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
        public ExtractRelevantTweet(String zone) { this.zone = zone; }
        @Override
        public void flatMap(String line, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            JsonNode tweet = new ObjectMapper().readTree(line);
            // Tweet language takes precedence over user interface language.
            String lang = tweet.path("lang").asText(tweet.path("user").path("lang").asText());
            if (!"en".equals(lang)) return;
            String text = tweet.path("extended_tweet").path("full_text").asText(
                tweet.path("full_text").asText(tweet.path("text").asText())).toLowerCase(Locale.ROOT);
            if (KEYWORDS.stream().noneMatch(text::contains)) return;
            String timestamp = tweet.path("created_at").asText();
            OffsetDateTime time;
            try { time = OffsetDateTime.parse(timestamp); }
            catch (java.time.format.DateTimeParseException e) {
                time = OffsetDateTime.parse(timestamp, TWITTER_TIME);
            }
            String hour = time.atZoneSameInstant(ZoneId.of(zone))
                .format(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:00"));
            out.collect(Tuple3.of(device(tweet.path("source").asText()), hour, 1));
        }
        public static String device(String source) {
            String s = source.replaceAll("<[^>]*>", "").toLowerCase(Locale.ROOT);
            if (s.contains("iphone") || s.contains("ipad")) return "AppleMobile";
            if (s.contains("mac")) return "AppleMac";
            if (s.contains("android")) return "Android";
            if (s.contains("blackberry")) return "BlackBerry";
            if (s.contains("windows phone")) return "WindowsPhone";
            if (s.contains("web")) return "Web";
            return "Other";
        }
    }
}
