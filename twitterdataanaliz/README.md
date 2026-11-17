# Twitter Veri Analizi

Apache Flink ile çevre ve kirlilik konulu İngilizce tweetleri cihaz ve saate göre sayan bir uygulama. Yerel JSONL dosyasındaki örnek verileri işler.

## Nasıl çalışır?

JSON verisi → dil ve anahtar kelime filtresi → cihaz ve saat bilgisi → `keyBy` → birikimli sayım.

Örnek veri `data/tweets-sample.jsonl` içinde 16 sentetik kayıt içerir. Filtrelerden 13 kayıt geçer. Sonuçlar terminale ve `output/counts.csv` dosyasına yazılır. Her cihaz/saat grubu için son satır, grubun toplam tweet sayısını gösterir.

## Çalıştırma

Gereksinimler: Java 17 ve Maven.

Proje klasöründe:

```bash
bash run.sh
```

Farklı çıktı ve saat dilimi için:

```bash
bash run.sh data/tweets-sample.jsonl output/istanbul.csv Europe/Istanbul
```

Varsayılan saat dilimi UTC'dir.

## Flink arayüzü

Yerel Flink 2.3.0 kümesinde çalıştırmak için proje klasöründe:

```bash
export FLINK_HOME=/path/to/flink-2.3.0
"$FLINK_HOME/bin/start-cluster.sh"
mvn package
"$FLINK_HOME/bin/flink" run \
  -c com.berfin.flink.TwitterDataAnalysis \
  target/twitterdataanaliz-1.0-SNAPSHOT.jar \
  "$PWD/data/tweets-sample.jsonl" \
  "$PWD/output/counts.csv"
```

Flink kümesi zaten çalışıyorsa başlatma adımını atlayabilirsin.

Arayüz: [localhost:8081](http://localhost:8081). Veri küçük olduğu için iş kısa sürede tamamlanır; **Completed Jobs** bölümünden operatörleri ve iş metriklerini inceleyebilirsin.
