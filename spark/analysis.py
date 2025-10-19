"""
Spark Analiz Görevi - Log Analitik Hattı
=========================================
Bu betik uygulama logları üzerinde kapsamlı analizler yapar:
1. Performans metrikleri (yanıt süreleri, yüzdelikler)
2. Hata analizi (hata oranları, dağılımlar)
3. Trafik desenleri (bölge, servis, endpoint bazında)
4. Kullanıcı davranış analizi

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum as spark_sum, min as spark_min, max as spark_max,
    percentile_approx, when, hour, to_timestamp, countDistinct,
    explode, split, year, month, dayofmonth
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys
import time
from datetime import datetime

# ====================================
# Configuration
# ====================================
class Config:
    """Spark görevi için yapılandırma sınıfı"""
    
    # Depolama Yapılandırması (YALNIZCA BİRİNİ seçin)
    STORAGE_TYPE = "hdfs"  # Seçenekler: "minio" veya "hdfs"
    
    # MinIO Yapılandırması
    # Docker ağı içindeyken 'minio', dışarıdan erişimde 'localhost' kullanılır
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin123"
    MINIO_BUCKET = "logs"
    MINIO_INPUT_PATH = "s3a://logs/raw/*.json"
    
    # HDFS Yapılandırması
    HDFS_INPUT_PATH = "hdfs://namenode:9000/logs/raw/*.json"
    
    # Çıktı (sonuç) yapılandırması
    OUTPUT_PATH_MINIO = "s3a://analytics/results"
    OUTPUT_PATH_HDFS = "hdfs://namenode:9000/analytics/results"
    
    # Spark Yapılandırması
    APP_NAME = "LogAnalytics"
    EXECUTOR_MEMORY = "2g"
    DRIVER_MEMORY = "1g"

# ====================================
# Spark Session Initialization
# ====================================
def create_spark_session(config):
    """
    Depolama tipine göre Spark oturumu oluştur ve yapılandır
    
    Args:
        config: Yapılandırma nesnesi
    Returns:
        SparkSession nesnesi
    """
    builder = SparkSession.builder \
        .appName(config.APP_NAME) \
        .config("spark.executor.memory", config.EXECUTOR_MEMORY) \
        .config("spark.driver.memory", config.DRIVER_MEMORY)
    
    # Configure for MinIO/S3
    if config.STORAGE_TYPE == "minio":
        # Add required JARs for S3A support
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

# ====================================
# Data Loading
# ====================================
def load_logs(spark, config) -> 'DataFrame':
    """
    JSON loglarını depodan yükle

    Args:
        spark: SparkSession nesnesi
        config: Yapılandırma nesnesi
    Returns:
        DataFrame: Logları içeren PySpark DataFrame
    """
    print("\n" + "="*60)
    print("📥 LOG'LAR DEPOLAMADAN YÜKLENİYOR")
    print("="*60)

    input_path = config.MINIO_INPUT_PATH if config.STORAGE_TYPE == "minio" else config.HDFS_INPUT_PATH
    print(f"Depolama Tipi: {config.STORAGE_TYPE.upper()}")
    print(f"Girdi Yolu: {input_path}")

    start_time = time.time()
    # TODO: Burada log verisini yükleyip DataFrame olarak döndürünüz
    # İpucu: spark.read.schema(schema).json(input_path) ile veri okuyabilirsiniz
    # İpucu: withColumn ile timestamp, hour, date gibi ek kolonlar ekleyebilirsiniz
    # İpucu: df.cache() ile DataFrame'i önbelleğe alabilirsiniz
    # İpucu: print ile yüklenen kayıt sayısını ve süreyi ekrana yazdırabilirsiniz
    load_time = time.time() - start_time
    # TODO: print(f"\n✅ {{total_records:,}} log kaydı yüklendi")
    # TODO: print(f"⏱️  Yükleme süresi: {{load_time:.2f}} saniye")
    # TODO: print(f"📊 Saniye başına kayıt: {{total_records/load_time:,.0f}}")
    # TODO: return df  # DataFrame döndürmelisiniz
    pass

# ====================================
# Analysis Functions
# ====================================

def analyze_response_times(df) -> 'DataFrame':
    """
    Endpoint bazında yanıt süresi metriklerini analiz et

    Hesaplananlar:
    - Ortalama yanıt süresi
    - Medyan (50. yüzdelik)
    - %95 yüzdelik
    - %99 yüzdelik
    - Min/Maks yanıt süreleri

    Returns:
        DataFrame: Endpoint bazında yanıt süresi metriklerini içeren PySpark DataFrame
        # Örnek kolonlar:
        # endpoint | request_count | avg_response_time | median_response_time | p95_response_time | p99_response_time | min_response_time | max_response_time
    """
    # TODO: Her endpoint için gerekli metrikleri hesaplayan kodu yazınız
    # İpucu: groupBy("endpoint") ile gruplayıp, count, avg, percentile_approx, min, max fonksiyonlarını kullanabilirsiniz
    pass


def analyze_errors(df) -> dict:
    """
    Hata desenlerini ve oranlarını analiz et

    Hesaplananlar:
    - Servis bazında hata oranı
    - Endpoint bazında hata oranı
    - Hata türüne göre dağılım
    - Saatlik hata trendleri

    Returns:
        dict: Sonuç DataFrame'lerini içeren bir sözlük
        # Örnek anahtarlar ve DataFrame kolonları:
        # {
        #   'service_errors': DataFrame[service, total_requests, error_count, warn_count, error_rate, warn_rate],
        #   'endpoint_errors': DataFrame[endpoint, total_requests, error_count, error_rate],
        #   'error_distribution': DataFrame[error_code, occurrence_count],
        #   'hourly_errors': DataFrame[hour, total_requests, error_count, error_rate]
        # }
    """
    # TODO: Hata analizlerini hesaplayan kodu yazınız
    # İpucu: groupBy, agg, withColumn, filter fonksiyonlarını kullanabilirsiniz
    pass


def analyze_traffic(df) -> dict:
    """
    Trafik desenlerini analiz et

    Hesaplananlar:
    - Bölgeye göre istekler
    - Servise göre istekler
    - En çok istek yapan kullanıcılar
    - En çok istek yapan IP'ler
    - Saatlik istek dağılımı

    Returns:
        dict: Sonuç DataFrame'lerini içeren bir sözlük
        # Örnek anahtarlar ve DataFrame kolonları:
        # {
        #   'region_traffic': DataFrame[region, request_count, avg_response_time],
        #   'service_traffic': DataFrame[service, request_count, unique_users, avg_response_time],
        #   'top_users': DataFrame[user_id, request_count],
        #   'top_ips': DataFrame[ip, request_count, unique_users],
        #   'hourly_traffic': DataFrame[hour, request_count, avg_response_time]
        # }
    """
    # TODO: Trafik analizlerini hesaplayan kodu yazınız
    # İpucu: groupBy, agg, orderBy, limit fonksiyonlarını kullanabilirsiniz
    pass


def analyze_status_codes(df) -> dict:
    """
    HTTP durum kodu dağılımını analiz et

    Returns:
        dict: Sonuç DataFrame'lerini içeren bir sözlük
        # Örnek anahtarlar ve DataFrame kolonları:
        # {
        #   'status_distribution': DataFrame[status_code, count, percentage],
        #   'endpoint_status': DataFrame[endpoint, status_code, count]
        # }
    """
    # TODO: Durum kodu analizlerini hesaplayan kodu yazınız
    # İpucu: groupBy, agg, withColumn fonksiyonlarını kullanabilirsiniz
    pass


def analyze_user_behavior(df) -> 'DataFrame':
    """
    Kullanıcı davranış desenlerini analiz et

    Returns:
        DataFrame: Kullanıcı davranış metriklerini içeren PySpark DataFrame
        # Örnek kolonlar:
        # user_id | total_requests | unique_endpoints | avg_response_time | error_count | unique_ips | error_rate
    """
    # TODO: Kullanıcı davranış analizini hesaplayan kodu yazınız
    # İpucu: groupBy, agg, withColumn, filter fonksiyonlarını kullanabilirsiniz
    pass

# ====================================
# Save Results
# ====================================
def save_results(results_dict, config):
    """
    Analiz sonuçlarını Parquet formatında depolamaya kaydet

    Args:
        results_dict: Kaydedilecek DataFrame sözlüğü
        config: Yapılandırma nesnesi
    """
    print("\n" + "="*60)
    print("💾 SONUÇLAR KAYDEDİLİYOR")
    print("="*60)

    output_base = config.OUTPUT_PATH_MINIO if config.STORAGE_TYPE == "minio" else config.OUTPUT_PATH_HDFS

    for name, df in results_dict.items():
        output_path = f"{output_base}/{name}"
        print(f"\n{name} kaydediliyor: {output_path}")
        try:
            df.write \
                .mode("overwrite") \
                .parquet(output_path)
            print(f"✅ {name} başarıyla kaydedildi")
        except Exception as e:
            print(f"❌ {name} kaydedilirken hata: {e}")


def generate_summary_report(df, start_time) -> None:
    """
    Genel özet istatistikleri üretir ve ekrana yazdırır

    Returns:
        None
    # Örnek çıktı:
    # Toplam İstek: ...
    # Toplam Hata: ...
    # Toplam Uyarı: ...
    # Benzersiz Kullanıcı: ...
    # Benzersiz IP: ...
    # Ortalama Yanıt Süresi: ...
    # Tarih Aralığı: ...
    # Toplam Çalışma Süresi: ...
    # İşleme Hızı: ...
    """
    # TODO: DataFrame'den özet istatistikleri hesaplayıp ekrana yazdırınız
    # İpucu: count, filter, distinct, agg fonksiyonlarını kullanabilirsiniz
    pass


# ====================================
# Main Execution
# ====================================
def main():
    """
    Ana çalıştırma fonksiyonu
    """
    print("\n" + "="*60)
    print("🚀 LOG ANALİZ HATTI BAŞLIYOR")
    print("="*60)
    print(f"Başlangıç Zamanı: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

    start_time = time.time()
    config = Config()

    # 1. Initialize Spark
    spark = create_spark_session(config)
    print(f"\n✅ Spark Oturumu Oluşturuldu: {spark.sparkContext.applicationId}")

    try:
        # 2. Veriyi yükle
        df = load_logs(spark, config)

        # 3. Analizleri çalıştır
        response_time_metrics = analyze_response_times(df)
        error_analysis = analyze_errors(df)
        traffic_analysis = analyze_traffic(df)
        status_analysis = analyze_status_codes(df)
        user_behavior = analyze_user_behavior(df)

        # 4. Kaydedilecek sonuçları hazırla (Düzenleyebilirsiniz)
        results_to_save = {
            "response_time_metrics": response_time_metrics,
            "service_errors": error_analysis["service_errors"],
            "endpoint_errors": error_analysis["endpoint_errors"],
            "error_distribution": error_analysis["error_distribution"],
            "hourly_errors": error_analysis["hourly_errors"],
            "region_traffic": traffic_analysis["region_traffic"],
            "service_traffic": traffic_analysis["service_traffic"],
            "top_users": traffic_analysis["top_users"],
            "hourly_traffic": traffic_analysis["hourly_traffic"],
            "status_distribution": status_analysis["status_distribution"],
            "user_behavior": user_behavior
        }

        # 5. Sonuçları kaydet
        save_results(results_to_save, config)

        # 6. Özet oluştur
        generate_summary_report(df, start_time)

        print("\n" + "="*60)
        print("✅ ANALİZ BAŞARIYLA TAMAMLANDI")
        print("="*60)

    except Exception as e:
        print(f"\n❌ Analiz sırasında hata: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()
        print(f"\n🛑 Spark Oturumu Sonlandırıldı")


if __name__ == "__main__":
    main()