# Sparkling Water Platformunda Veri Madenciliği Adımlarının Gerçeklenmesi

Bu projede veri madenciliği adımlarında kullanılan algoritmaların bir kısmı Sparkling Water Platformunda uygulanmış, bir kısmı da bu platform için geliştirilmiştir.

Veri Madenciliği Adımları ve Kullanılan Algoritmalar:

1. Aykırı Değer Ayıklama: Mahalanobis Uzaklığı
2. Eksik Veri Tamamlama:  Doğrusal Regresyon, Çoklu İmputasyon
3. Boyut İndirgeme: Temel Bileşen Analizi (PCA)
4. Özellik Seçimi : Bilgi Kazancı Yöntemi
5. Sınıflandırma: Logistik Regresyon
6. Kümeleme: K-Means
7. Birliktelik Kuralları: Apriori, FP-Growth
8. Toplu Öğrenme: Stacking

H2O'da Doğrusal Regresyon, Temel Bileşen Analizi (PCA), Lojistik Regresyon, K-Means, Stacking hazır olarak bulunmaktaydı. H2O'daki makine öğrenmesi kütüphaneleri kullanılarak uygulanmıştır.

FP-Growth Apache Spark'ta hazır olarak bulunmaktaydı. Spark MLlib kullanılarak uygulanmıştır.

Mahalanobis Mesafesi hesaplama, Çoklu İmputasyon, Bilgi Kazancı yöntemi, Apriori yöntemleri Apache Spark ve H2O veri işleme fonksiyonları kullanılarak geliştirilmiştir.

Projede kullanıcı kullanımı sunmak için arayüz eklenmiştir. Arayüz Java swing kullanılarak yapılmıştır. Ancak algoritmaların uygulanması ve geliştirilmesi için Python programlama dili tercih edilmiştir. Arayüzde ilgili algoritma çalıştırılmak istendiğinde arka planda Python scriptleri koşturulmaktadır.

## Sistemin Kurulması

Öncelikle sistemde HDFS (Hadoop Distributed File System) kullanılabilmesi için hadoop platformunun kurulması gerekmektedir. 

1- Ubuntu Üzerinde Hadoop Kurulumu yapılır. Aşağıdaki linkteki adımlar takip edilerek sanal makineler üzerine Hadoop ve HDFS kurulur.

https://medium.com/@dauut/ubuntu-%C3%BCzerine-da%C4%9F%C4%B1t%C4%B1k-multi-node-hadoop-2-7-2-ve-hbase-1-2-0-kurulumu-56190d81baa5

2- Spark kurulumu için Sanal makineler kullanılıyorsa her bir sanal makineye Spark yüklenmelidir. https://spark.apache.org/downloads.html sitesinden Apache Spark indirilir. Spark'ın bulunduğu dosya yolu sisteme kaydedilir. (Terminale ~/.bashrc komutunu yaz ve dosya yollarını ekle)

Apache Spark'ın Hadoop kurulu bir sisteme kurulmasının basitçe anlatıldığı site: https://chongyaorobin.wordpress.com/2015/07/01/step-by-step-of-installing-apache-spark-on-apache-hadoop/

3- pip3 install h2o_pysparkling_2.2 komutu ile pysparkling yüklenir. (Anaconda varsa uygun komut internetten bulunmalı)

4- pip3 install findspark ile Python kodlarında kullanılan findspark kütüphanesi yüklenir.

Ve kurulum tamamlanır.

## Çalıştırılması :

Öncelikle Hadoop ve Spark platformları aktif hale getirilmelidir.

Bunun için öncelikle **HDFS'i çalıştırmak için terminale start-all.sh** yazılır.

Ardından terminale yazılan jps komutu ile DataNode, ResourceManager, NameNode, SecondaryNameNode, NodeManager portlarının aktif hale geldiği görülür.

Ayrıca browser'dan localhost:50070 ile HDFS arayüzüne erişilebilir ve oradaki node sayıları, veri setleri vb. bilgiler görüntülenebilir.

Spark'ı çalıştırmak için öncelikle Spark'ın yüklü olduğu klasöre gidilir ve sbin klasörüne girilir. 

Ardından **./start-all.sh** komutu ile Spark'ın Master ve Slave node'ları aktif hale getirilir.

Browser'a localhost:8080 yazılarak Spark'ın arayüzüne ulaşılabilir ve çalışan node'lar görüntülenebilir. Local'de sadece 1 node çalışır. Sanal makinelere kurulan diğer Spark'lar ile birden çok node olabilir. 

Projede GUI jar dosyası üzerinden çalıştırılmalıdır. Netbeans veya eclipse üzerinden çalıştırıldığında sistem Spark, Hadoop dosya yollarını göremeyeceğinden hata verebilir. 

Kullanıcı arayüzünde çalıştırılan her bir algoritma için arka planda Python scripti çalışmaktadır. Dosya yolu bulunamadı gibi bir hata durumunda kod içerisindeki Python kodlarının dosya yolu kontrol edilmelidir.

NOT: Kod üzerinden arayüz çalıştırılmak istenirse önce kaynak kodunda python scriptlerinin bulunduğu dosya yolu ve jar dosyasının bulunduğu dosya yolu güncellenmelidir. 'Clean and build' ile yeni jar dosyası oluşturulmalıdır. Oluşturulan .jar dosyasının bulunduğu klasöre bir üst klasörde bulunan HdfsReader.jar dosyası kopyalanmalıdır. Çünkü GUI'de veri seti görüntülenmek istendiğinde HdfsReader.jar dosyası kullanılmaktadır. Ve en son terminalden jar dosyası çalıştırılmalıdır.

Proje çalıştırılması tamamlandığında bilgisayarı kapatmadan:

1- /home/cansu/spark/spark-2.2.1-bin-hadoop2.7/sbin dosya yoluna gidilmeli ve **./stop-all.sh** komutu ile Spark kapatılmalı.

2- Sonra **stop-all.sh** komutu ile HDFS kapatılmalı. 
