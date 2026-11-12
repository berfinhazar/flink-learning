# Session window örneği

Gereksinimler: Java, Maven ve çalışan yerel Flink kümesi. POM Flink 1.20.0 için hazırlanmıştır. Kurulu sürümü `flink --version` ile kontrol edin; farklı bir 1.x sürümünde pom.xml içindeki flink.version değerini eşleştirin. Flink 2.x için API uyarlaması gerekir.

## Terminal 1 — bu klasörde
```bash
javac DataServer.java
java DataServer avg.txt
```
Sunucu Flink bağlanana kadar bekler. Her 10 kayıttan sonra 2 saniye durur; böylece 1 saniyelik oturum boşluğu gözlemlenir. Her yeniden deneme öncesinde sunucuyu yeniden başlatın.

## Terminal 2 — bu klasörde
```bash
mvn clean package
flink run -c AverageProfitSessionWindowProcessingTime -p 1 target/session-window.jar
```
Çalışan küme yoksa Flink kurulumunuzdaki bin/start-cluster.sh ile başlatın.

## Çıktı
print() çıktısı yerel kümenin TaskManager .out dosyasında görünür. Flink kurulumunun log klasöründeki taskexecutor veya taskmanager .out dosyasını açın; gerekirse `tail -f DOSYA_YOLU` kullanın.

Tuple sırası: (ay, kategori, ürün, kâr toplamı, kayıt sayısı). Hesap ay bazındadır; kategori/ürün alanları gruplama anahtarı değildir. Kod orijinal dersteki gibi toplam ve sayı üretir, ortalama üretmez. Ortalama = kâr toplamı / kayıt sayısı.

Kaynak ve çıktı yollarındaki hocanın bilgisayarına ait sabit yollar kaldırıldı. Veri sunucusu sonlu örnek veriyi gönderip kapanır.
