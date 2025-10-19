#!/bin/bash
# Geliştirme ortamı konteynerine bağlanma betiği
# Bu betik konteyneri başlatır, gerekli paketleri yükler ve interaktif shell açar.

# bigdata-network ağının var olup olmadığını kontrol et, yoksa oluştur
echo "Docker ağı kontrol ediliyor..."
if ! docker network ls --filter name=bigdata-network --quiet | grep -q .; then
    echo "bigdata-network ağı bulunamadı, oluşturuluyor..."
    docker network create bigdata-network
    echo "bigdata-network ağı başarıyla oluşturuldu."
else
    echo "bigdata-network ağı zaten mevcut."
fi

echo "Geliştirme ortamı konteyneri oluşturuluyor / başlatılıyor (varsa atlanır)..."
docker-compose -f docker/docker-compose-env.yml up -d
echo "Konteynere bağlanılıyor..."
echo "Java ve Python paketleri yükleniyor (ilk çalışmada birkaç dakika sürebilir)..."
docker exec -it dev-env bash -c "pip install -r /workspace/requirements.txt"
echo ""

docker exec -it dev-env bash
