
## Takım Adı: <!-- Takım adınızı buraya yazın -->

### Takım Üyeleri
- <!-- Üye 1 adı -->
- <!-- Üye 2 adı -->
- <!-- Üye 3 adı -->
- <!-- Üye 4 adı -->

# Performans ve Analiz Raporu

Bu dosya, ödevinizin raporunu oluşturmanız için bir şablondur. Aşağıdaki başlıkları doldurarak kendi bulgularınızı, karşılaştırmalarınızı ve analizlerinizi ekleyin.

---

## 1. Depolama Sistemi Seçimi

| Alan                      | Açıklama |
|---------------------------|----------|
| **Seçtiğim Depolama Sistemi** | <!-- HDFS veya MinIO --> |
| **Gerekçem**              | <!-- Neden bu sistemi seçtiğinizi yazın --> |
| **Kurulum Notları**       | <!-- Kurulumda yaşadığınız sorunlar ve çözümler --> |
| **Erişilebilirlik Testi** | <!-- Dosyaların erişimini nasıl doğruladınız? --> |

---

## 2. Spark Analiz Sonuçları

Aşağıdaki analizleri tablo veya grafiklerle özetleyin:

- Endpoint bazında ortalama yanıt süresi
- 95\. yüzdelik yanıt süresi
- Servis bazında hata oranları
- Bölgeye göre istek dağılımı
- En çok istek yapan kullanıcılar ve IP'ler

**Örnek Tablo:**

| Endpoint | Ortalama Yanıt Süresi | 95. Yüzdelik | Hata Oranı |
|----------|----------------------|--------------|------------|
| /login   | 120 ms               | 300 ms       | %0.5       |
| /search  | 80 ms                | 200 ms       | %0.2       |

---

## 3. Format Karşılaştırması

| Format   | Dosya Boyutu | Sorgu Süresi | Açıklama |
|----------|-------------|--------------|----------|
| JSON     | ...         | ...          | ...      |
| Parquet  | ...         | ...          | ...      |
| ORC      | ...         | ...          | ...      |

- Hangi formatın neden daha verimli olduğunu açıklayın.
- Sıkıştırma ve bölümleme etkisini belirtin.

---

## 4. Optimizasyon Sonuçları

| Sorgu Tipi           | Süre (sn) | Bellek Kullanımı | Yanlış Pozitif Oranı |
|----------------------|-----------|------------------|----------------------|
| Standart Sorgu       | ...       | ...              | -                    |
| Bloom Filter ile     | ...       | ...              | ...                  |
| Bitset ile           | ...       | ...              | ...                  |

- Bloom Filter ve Bitset kullanımı ile elde edilen performans iyileşmelerini açıklayın.

---

## 5. HBase Tablo Tasarımı

- Satır anahtarı yapısı ve kolon aileleri hakkında kısa açıklama
- Veri yükleme adımlarınız
- Sorgu örnekleri

---

## 6. Dashboard ve Görselleştirme

- Hangi grafikler ve görselleştirmeler kullandınız?
- Dashboard'un ekran görüntüsünü ekleyin (`reports/` klasörüne görsel ekleyin ve buraya referans verin)
- Dashboardda sunduğunuz filtreleme ve dışa aktarma özelliklerini açıklayın

---

## 7. Karşılaşılan Sorunlar ve Çözümler

- Kurulumda, kodda veya analizde yaşadığınız teknik sorunlar (varsa)
- Nasıl çözdüğünüzü ve öğrendiklerinizi yazın

---

## 8. Ekstra Analizler ve Bonuslar

- Eklediğiniz ekstra analizler veya karşılaştırmalar
- Bonus puan için yaptığınız ek çalışmalar

---

## 9. Sonuç ve Değerlendirme

- Proje boyunca öğrendikleriniz
- Kendi yorumunuz ve önerileriniz

---

## 10. Kaynaklar

- Kullandığınız dokümantasyon, makale veya web siteleri

---



**Not:** Tabloları, kod örneklerini ve görselleri ekleyerek raporunuzu zenginleştirebilirsiniz. Her başlığı doldurmayı unutmayın!
