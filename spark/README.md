
# Spark Analiz Modülü

Bu modül, web sunucu loglarını analiz etmek için Apache Spark kullanır. Log verilerini işleyerek performans, hata, trafik ve kullanıcı davranışı gibi metrikler üretir.

---

## Ortam Kurulumu

1. **Docker ile geliştirme ortamını başlatın:**
   ```bash
   docker-compose -f docker/docker-compose-env.yml up -d
   ```
2. **Konteynere bağlanın:**
   ```bash
   ./scripts/connect_dev_env.sh
   ```
   veya Windows için:
   ```powershell
   .\scripts\connect_dev_env.ps1
   ```
3. **Gerekli Python paketleri otomatik yüklenir.**

---


## Analiz Scriptlerini Çalıştırma ve Geliştirme

> **Not:** Script dosyalarında sadece başlangıç (starter) kodu sağlanır. Yani Spark oturumu başlatma, HDFS/MinIO bağlantısı gibi temel altyapı kodları hazırdır. Analiz, istatistik ve raporlama ile ilgili asıl kodları kullanıcı kendisi yazacaktır.

### 1. JSON'dan Parquet/ORC'ye Dönüştürme
```bash
python spark/convert_from_json.py
```
Bu scriptte starter kod olarak Spark oturumu ve dosya okuma/yazma örneği bulunur. Dönüştürme ve analiz mantığını siz geliştireceksiniz.

### 2. Log Analizi
```bash
python spark/analysis.py
```
Starter kodda Spark bağlantısı ve veri okuma örneği vardır. Performans, hata, trafik ve kullanıcı davranışı analizlerini siz ekleyeceksiniz.

### 3. Optimizasyon Teknikleri
```bash
python spark/optimization.py
```
Starter kodda Bloom Filter, Bitset ve Broadcast Join altyapısı örneklenmiştir. Kendi optimizasyon ve test kodlarınızı ekleyebilirsiniz.

---

## Format Karşılaştırması (Örnek Sonuçlar)

| Format   | Kayıt Sayısı | Okuma Süresi | Okuma Hızı (kayıt/sn) |
|----------|--------------|--------------|-----------------------|
| JSON     | 5,000,000    | 2.67s        | 1,870,634             |
| Parquet  | 5,000,000    | 0.13s        | 37,246,482            |
| ORC      | 5,000,000    | 0.11s        | 47,042,230            |

---

## Özet Analiz Raporu (Örnek)

```
📋 ÖZET RAPOR
============================================================
📊 Genel İstatistikler:
   Toplam İstek: 5,000,000
   Toplam Hata: 387,364 (7.75%)
   Toplam Uyarı: 875,299 (17.51%)
   Benzersiz Kullanıcı: 1,000
   Benzersiz IP: 200
   Ortalama Yanıt Süresi: 273.20ms
   Tarih Aralığı: 2025-10-19 12:17:14.560392 - 2025-10-19 12:17:15.560988

⏱️  Toplam Çalışma Süresi: 34.18 saniye
   İşleme Hızı: 146,283 kayıt/saniye
```

---

## Notlar
- Tüm scriptler Türkçe açıklamalı ve kullanıcı dostudur.
- Analiz sonuçları `analytics/results` klasörüne Parquet formatında kaydedilir, daha sonra `hbase/load_data.py` ile HBase'e yüklenir.
- Ortamda Spark, Python ve Java otomatik kurulu gelir.
- Daha fazla örnek ve açıklama için script dosyalarındaki yorumları inceleyin.
