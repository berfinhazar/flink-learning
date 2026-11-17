# Flink Learning

Apache Flink ile veri akışlarını işleme ve analiz etme üzerine geliştirdiğim Java projeleri. Temel kelime sayımından pencereleme, Twitter veri analizi ve banka işlemlerinde şüpheli hareket tespitine uzanan uygulamalar içerir.

## Projeler

| Proje | Açıklama |
|---|---|
| [Kelime Sayımı](src/main/java/com/berfin/flink/StreamingWordCount.java) | Veri akışındaki kelimelerin sayılması |
| [Taksi Yolculuğu Analizi](src/main/java/com/berfin/flink/CabTripAnalytics.java) | Yolculuk verileri üzerinde akış analizi |
| [Session Window](session-window/) | Oturum pencereleriyle veri işleme |
| [Twitter Veri Analizi](twitterdataanaliz/) | Çevre konulu tweetlerin cihaz ve saate göre analizi |
| [Banka İşlemleri Analizi](bankarealdataanaliz/) | Dört kurala göre şüpheli işlemlerin tespiti |

Session Window, Twitter ve banka projelerinin çalıştırma adımları kendi klasörlerindeki README dosyalarında yer alır. Twitter ve banka uygulamalarında Java 17, Maven ve Apache Flink 2.3.0 kullanılır.
