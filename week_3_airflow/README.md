### Ödev İçeriği
    Flow.png içeriğindeki adımları airflow dag içinde tasarlayınız. 5 dakikada bir çalışmalı.
    db connection'ları değiştirmenize gerek yok, mevcutları kullanabilirsiniz. yeni collection üretilecek yerelerde kendi
    adınızın olduğu collectionları oluşturabilirsiniz.
    mongodb de sıcaklık ve nem tablosuna da kayıt ataraken kendi creator bilgisine kendi adınızı ekleyin.


### Çalıştırmak için

    docker compose -f docker-compose.yaml build
    docker compose -f docker-compose.yaml up

### Durdurmak için

    docker compose -f docker-compose.yaml down
