"""
Spark Optimizasyon Teknikleri - ÖDEV ŞABLONu
============================================
GÖREV: Bloom Filter, Bitset ve Broadcast Join optimizasyon tekniklerini kullanarak
log analizi performansını artırın.

ÖĞRENİLECEKLER:
- Bloom Filter: Olasılıksal veri yapısı ile hızlı üyelik testi
- Bitset: Bellek dostu bitmap ile filtreleme
- Broadcast Join: Küçük tablolarla join optimizasyonu
- Performans ölçümü ve karşılaştırma teknikleri

TAMAMLANMASI GEREKEN GÖREVLER:
1. BloomFilter sınıfı - Hash tabanlı olasılıksal veri yapısı
2. IPBitset sınıfı - IP adresi için bitmap
3. demonstrate_bloom_filter() - Bloom Filter ile user_id sorgulaması
4. demonstrate_bitset() - Bitset ile IP filtreleme
5. demonstrate_broadcast_join() - Küçük tablo join optimizasyonu

İPUCLARI:
- Bloom Filter: Multiple hash functions, yanlış pozitif olabilir
- Bitset: Her eleman için 1 bit, O(1) erişim
- Broadcast: df.join(broadcast(small_df), "key")
- time.time() ile performans ölçümü yapın

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
import time
from datetime import datetime
import hashlib

# ====================================
# YAPABDIRILMIŞ KISIM - Yapılandırma
# ====================================
class Config:
    """Optimizasyon gösterimleri için yapılandırma sınıfı - HAZIR"""
    
    # Depolama Yapılandırması
    STORAGE_TYPE = "hdfs"  # Seçenekler: "minio" veya "hdfs"
    
    # MinIO Yapılandırması
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin123"
    MINIO_INPUT_PATH = "s3a://logs/raw/*.json"
    
    # HDFS Yapılandırması
    HDFS_INPUT_PATH = "hdfs://namenode:9000/logs/raw/*.json"
    
    # Optimizasyon Yapılandırması
    BLOOM_FILTER_SIZE = 1000000  # Beklenen eleman sayısı
    BLOOM_FILTER_FPP = 0.01      # Yanlış pozitif olasılığı (1%)
    
    # Test için örnek user ID'ler
    SAMPLE_USER_IDS = [1, 5, 10, 25, 50, 100, 200, 500, 999]
    
    # Test için örnek IP adresleri
    SAMPLE_IPS = ["192.168.1.1", "192.168.1.5", "192.168.1.10"]
    
    # Spark Yapılandırması
    # ram miktarınız fazla ise bunlar ile oynayarak performansı iyileştirebilirsiniz
    APP_NAME = "LogOptimization"
    EXECUTOR_MEMORY = "2g"
    DRIVER_MEMORY = "1g"

# ====================================
# GÖREV 1: Bloom Filter Implementasyonu
# ====================================
class BloomFilter:
    """
    Bloom Filter - Olasılıksal Veri Yapısı
    
    YAPILACAKLAR:
    1. __init__: Bit array ve hash sayısını başlatın
    2. _hash: Seed ile MD5 hash üreten yardımcı fonksiyon
    3. add: Elemana ait tüm hash pozisyonlarını 1 yapın
    4. contains: Tüm hash pozisyonları 1 mi kontrol edin
    5. get_stats: Doluluk oranı ve istatistikleri hesaplayın
    
    BLOOM FILTER PRENSİBİ:
    - Multiple hash functions kullanır (num_hashes kadar)
    - Her eleman için her hash ile bit_array'de bir pozisyon işaretlenir
    - contains() kontrolünde TÜM pozisyonlar 1 olmalı
    - Yanlış pozitif olabilir (diyor ama yok) ama yanlış negatif ASLA olmaz
    
    İPUCLARI:
    - hashlib.md5() ile hash üretin
    - Hash değerini size'a göre modulo alın
    - Döngü ile num_hashes kadar hash hesaplayın
    """
    
    def __init__(self, size, num_hashes=3):
        """
        Bloom Filter'ı başlat
        
        TODO:
        1. self.size = size
        2. self.num_hashes = num_hashes
        3. self.bit_array = [0] * size (tüm bitler başlangıçta 0)
        """
        # TODO: Bloom Filter'ı başlatın
        pass
        
    def _hash(self, item, seed):
        """
        Bir eleman için seed ile hash üret
        
        TODO:
        1. f"{item}_{seed}" string'ini UTF-8'e encode edin
        2. hashlib.md5() ile hash hesaplayın
        3. .hexdigest() ile hex string alın
        4. int(hex, 16) ile integer'a çevirin
        5. size'a göre modulo alıp pozisyon döndürün
        """
        # TODO: Hash fonksiyonunu uygulayın
        pass
    
    def add(self, item):
        """
        Bloom Filter'a eleman ekle
        
        TODO:
        1. num_hashes kadar döngü yapın
        2. Her seed için _hash() çağırın
        3. Dönen index'te bit_array[index] = 1 yapın
        """
        # TODO: Elemanı Bloom Filter'a ekleyin
        pass
    
    def contains(self, item):
        """
        Eleman Bloom Filter'da var mı kontrol et
        
        TODO:
        1. num_hashes kadar döngü yapın
        2. Her seed için _hash() çağırın
        3. Eğer herhangi bir bit_array[index] == 0 ise False döndürün
        4. Tüm bitler 1 ise True döndürün
        
        NOT: True dönerse eleman muhtemelen vardır (yanlış pozitif olabilir)
             False dönerse eleman kesinlikle yoktur (yanlış negatif olmaz)
        """
        # TODO: Eleman üyeliğini kontrol edin
        pass
    
    def get_stats(self):
        """
        Bloom Filter istatistiklerini hesapla
        
        TODO:
        1. sum(bit_array) ile kaç bit'in 1 olduğunu sayın
        2. Fill ratio = bits_set / size hesaplayın
        3. Dictionary döndürün: size, bits_set, fill_ratio, num_hashes
        """
        # TODO: İstatistikleri hesaplayın
        pass


# ====================================
# GÖREV 2: IP Bitset Implementasyonu
# ====================================
class IPBitset:
    """
    IP Adresi Bitset - Bellek Dostu Bitmap
    
    YAPILACAKLAR:
    1. __init__: 256 elemanlı bitset başlatın (192.168.1.0-255 için)
    2. _ip_to_index: IP'nin son oktetini index olarak döndürün
    3. add: IP için ilgili bit pozisyonunu 1 yapın
    4. contains: IP'nin biti 1 mi kontrol edin
    5. get_stats: Kayıtlı IP sayısı ve bellek kullanımı
    
    BITSET PRENSİBİ:
    - Her olası değer için 1 bit kullanır
    - IP: 192.168.1.X için X değeri index olur (0-255)
    - O(1) ekleme ve sorgulama
    - Yanlış pozitif/negatif olmaz (deterministik)
    
    İPUCLARI:
    - ip.split('.')[-1] ile son okteti alın
    - int() ile string'i sayıya çevirin
    - Try/except ile hatalı IP'leri yakalayın
    """
    
    def __init__(self):
        """
        256 elemanlı Bitset başlat (192.168.1.0 - 192.168.1.255)
        
        TODO:
        1. self.bitset = [0] * 256 oluşturun
        """
        # TODO: Bitset'i başlatın
        pass
    
    def _ip_to_index(self, ip):
        """
        IP adresini index'e çevir (son oktet)
        
        TODO:
        1. ip.split('.') ile IP'yi parçalayın
        2. [-1] ile son okteti alın
        3. int() ile sayıya çevirin
        4. Hata durumunda None döndürün
        
        Örnek: "192.168.1.42" -> 42
        """
        # TODO: IP'yi index'e çevirin
        pass
    
    def add(self, ip):
        """
        Bitset'e IP adresi ekle
        
        TODO:
        1. _ip_to_index() ile index'i alın
        2. index None değilse bitset[index] = 1 yapın
        """
        # TODO: IP'yi Bitset'e ekleyin
        pass
    
    def contains(self, ip):
        """
        IP adresi Bitset'te var mı kontrol et
        
        TODO:
        1. _ip_to_index() ile index'i alın
        2. index None değilse bitset[index] == 1 mi kontrol edin
        3. Sonucu döndürün
        """
        # TODO: IP üyeliğini kontrol edin
        pass
    
    def get_stats(self):
        """
        Bitset istatistiklerini hesapla
        
        TODO:
        1. sum(bitset) ile kaç IP'nin kayıtlı olduğunu sayın
        2. Dictionary döndürün: total_slots, ips_registered, memory_bytes
        
        memory_bytes = len(bitset) * 1 (basitleştirilmiş, her eleman 1 byte)
        """
        # TODO: İstatistikleri hesaplayın
        pass


# ====================================
# Yardımcı Fonksiyonlar - HAZIR
# ====================================
def create_spark_session(config):
    """Spark oturumu oluştur ve yapılandır - HAZIR, değiştirmeyin"""
    builder = SparkSession.builder \
        .appName(config.APP_NAME) \
        .config("spark.executor.memory", config.EXECUTOR_MEMORY) \
        .config("spark.driver.memory", config.DRIVER_MEMORY)
    
    # Configure for MinIO/S3
    if config.STORAGE_TYPE == "minio":
        builder = builder \
            .config("spark.jars.packages", 
                   "org.apache.hadoop:hadoop-aws:3.3.4,"
                   "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                   "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    # Configure for HDFS
    elif config.STORAGE_TYPE == "hdfs":
        builder = builder \
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def load_logs(spark, config):
    """JSON loglarını depodan yükle - HAZIR, değiştirmeyin"""
    print("\n" + "="*60)
    print("📥 LOG'LAR YÜKLENİYOR")
    print("="*60)
    
    input_path = config.MINIO_INPUT_PATH if config.STORAGE_TYPE == "minio" else config.HDFS_INPUT_PATH
    print(f"Depolama Tipi: {config.STORAGE_TYPE.upper()}")
    print(f"Girdi Yolu: {input_path}")
    
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("service", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("level", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("ip", StringType(), True),
        StructField("region", StringType(), True),
        StructField("response_time", IntegerType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("error_code", StringType(), True),
        StructField("message", StringType(), True),
    ])
    
    df = spark.read.schema(schema).json(input_path)
    df.cache()
    
    total_records = df.count()
    print(f"\n✅ {total_records:,} kayıt yüklendi")
    
    return df


# ====================================
# GÖREV 3: Bloom Filter Demonstrasyonu
# ====================================
def demonstrate_bloom_filter(df, config):
    """user_id sorgulamalarında Bloom Filter optimizasyonunu göster"""
    print("\n" + "="*60)
    print("🔍 BLOOM FILTER OPTİMİZASYONU")
    print("="*60)
    
    print("\n1. Bloom Filter oluşturuluyor...")
    # TODO: GÖREV 3.1 - Bloom Filter oluştur ve doldur
    # İpucu: start_time = time.time() ile başla, build_time = time.time() - start_time ile bitir
    
    print("\n2. Bloom Filter sorguları test ediliyor...")
    # TODO: GÖREV 3.2 - Test kullanıcılarını kontrol et ve sonuçları yazdır
    # İpucu: Var olan + var olmayan user ID'lerle test et (config.SAMPLE_USER_IDS + [9999, 10000, 50000])
    
    print("\n3. Performans Karşılaştırması: Bloom Filter vs Doğrudan Sorgu")
    # TODO: GÖREV 3.3 - İki yöntemi karşılaştır
    # İpucu: Her iki yöntem için start_time ve end_time ile süre ölç, speedup hesapla



# ====================================
# GÖREV 4: Bitset Demonstrasyonu
# ====================================
def demonstrate_bitset(df, config):
    """IP adresi filtrelemede Bitset optimizasyonunu göster"""
    print("\n" + "="*60)
    print("🎯 BITSET OPTİMİZASYONU")
    print("="*60)
    
    print("\n1. IP Bitset oluşturuluyor...")
    # TODO: GÖREV 4.1 - IP Bitset oluştur ve doldur
    # İpucu: start_time = time.time() ile başla, build_time = time.time() - start_time ile bitir
    
    print("\n2. Bitset sorguları test ediliyor...")
    # TODO: GÖREV 4.2 - Test IP'lerini kontrol et ve sonuçları yazdır
    # İpucu: test_ips = config.SAMPLE_IPS + ["192.168.1.100", "192.168.1.200", "10.0.0.1"]
    
    print("\n3. Performans Karşılaştırması: Bitset vs Doğrudan Filtre")
    # TODO: GÖREV 4.3 - İki yöntemi karşılaştır
    # İpucu: Her iki yöntem için start_time ve end_time ile süre ölç, speedup hesapla


# ====================================
# GÖREV 5: Broadcast Join Demonstrasyonu
# ====================================
def demonstrate_broadcast_join(df, spark):
    """Broadcast join optimizasyonunu göster"""
    print("\n" + "="*60)
    print("📡 BROADCAST JOIN OPTİMİZASYONU")
    print("="*60)
    
    print("\n1. Servis Metadata Lookup Tablosu:")
    # TODO: GÖREV 5.1 - Küçük lookup tablosu oluştur
    # İpucu: spark.createDataFrame() ile 4 satırlık metadata tablosu oluştur
    
    print("\n2. Normal Join...")
    # TODO: GÖREV 5.2 - Normal join yap
    # İpucu: start_time ile başla, df.join() ile birleştir, regular_time hesapla
    
    print("\n3. Broadcast Join...")
    # TODO: GÖREV 5.3 - Broadcast join yap
    # İpucu: start_time ile başla, df.join(broadcast()) kullan, broadcast_time hesapla
    
    print("\n4. Performans Karşılaştırması:")
    # TODO: GÖREV 5.4 - Hız farkını hesapla
    # İpucu: speedup = regular_time / broadcast_time


# ====================================
# Ana Çalıştırma - TAMAMLAYIN
# ====================================
def main():
    """Ana çalıştırma fonksiyonu"""
    print("\n" + "="*60)
    print("🚀 SPARK OPTİMİZASYON TEKNİKLERİ - ÖDEV")
    print("="*60)
    print(f"Başlangıç Zamanı: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    start_time = time.time()
    config = Config()
    
    try:
        # TODO: Spark oturumu oluştur
        # spark = create_spark_session(config)
        # df = load_logs(spark, config)
        
        # TODO: Optimizasyon demonstrasyonlarını çalıştır
        # demonstrate_bloom_filter(df, config)
        # demonstrate_bitset(df, config)
        # demonstrate_broadcast_join(df, spark)
        
        # execution_time = time.time() - start_time
        # print(f"\n⏱️  Toplam Çalışma Süresi: {execution_time:.2f} saniye")
        
        print("\n" + "="*60)
        print("✅ OPTİMİZASYON DEMOSU TAMAMLANDI")
        print("="*60)

    except Exception as e:
        print(f"\n❌ Hata: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        # TODO: Spark session'ı durdur
        print("\n🛑 İşlem tamamlandı")


if __name__ == "__main__":
    main()
