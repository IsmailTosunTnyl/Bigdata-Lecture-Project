# Geliştirme ortamı konteynerine bağlanma betiği
# Bu betik: konteyneri oluşturur/başlatır, gerekirse paketleri kurar ve
# interaktif bir bash oturumu açar.

# bigdata-network ağının var olup olmadığını kontrol et, yoksa oluştur
Write-Host "Docker ağı kontrol ediliyor..." -ForegroundColor Yellow
$networkExists = docker network ls --filter name=bigdata-network --quiet
if (-not $networkExists) {
    Write-Host "bigdata-network ağı bulunamadı, oluşturuluyor..." -ForegroundColor Cyan
    docker network create bigdata-network
    Write-Host "bigdata-network ağı başarıyla oluşturuldu." -ForegroundColor Green
} else {
    Write-Host "bigdata-network ağı zaten mevcut." -ForegroundColor Green
}

Write-Host "Geliştirme ortamı konteyneri oluşturuluyor / başlatılıyor (varsa atlanır)..." -ForegroundColor Green
docker-compose -f docker/docker-compose-env.yml up -d
Write-Host "Konteynere bağlanılıyor..." -ForegroundColor Green
Write-Host "Java ve Python paketleri yükleniyor (ilk çalışmada birkaç dakika sürebilir)..." -ForegroundColor Cyan
docker exec -it dev-env bash -c "pip install -r /workspace/requirements.txt"
Write-Host ""

docker exec -it dev-env bash
