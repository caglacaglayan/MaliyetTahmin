import datetime
import psycopg2
from hijridate import Hijri


# Veritabanına bağlantı kurma
conn = psycopg2.connect(
    database="shepherd",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)


def hafta_sonu_gunleri(yil):
    baslangic_tarihi = datetime.date(yil, 1, 1)  # Yılın başlangıç tarihi
    bitis_tarihi = datetime.date(yil, 12, 31)  # Yılın bitiş tarihi
    gun_sayisi = (bitis_tarihi - baslangic_tarihi).days + 1  # Yılın toplam gün sayısı
    hafta_sonu_sayisi = 0

    for i in range(gun_sayisi):
        gun = baslangic_tarihi + datetime.timedelta(days=i)
        if gun.weekday() >= 5:  # Cumartesi veya Pazar
            hafta_sonu_sayisi += 1

    return hafta_sonu_sayisi


def hafta_ici_resmi_tatiller(yil):
    # Türkiye'deki resmi tatillerin listesi
    resmi_tatiller = [
        datetime.date(yil, 1, 1),   # Yılbaşı
        datetime.date(yil, 4, 23),  # Ulusal Egemenlik ve Çocuk Bayramı
        datetime.date(yil, 5, 1),   # Emek ve Dayanışma Günü
        datetime.date(yil, 5, 19),  # Atatürk'ü Anma, Gençlik ve Spor Bayramı
        datetime.date(yil, 7, 15),  # Demokrasi ve Milli Birlik Günü
        datetime.date(yil, 8, 30),  # Zafer Bayramı
        datetime.date(yil, 10, 29)  # Cumhuriyet Bayramı
    ]

    hafta_ici_tatil_sayisi = 0
    for tatil in resmi_tatiller:
        if tatil.weekday() < 5:
            hafta_ici_tatil_sayisi += 1

    return hafta_ici_tatil_sayisi


def hafta_ici_bayram_gunleri(yil):
    year = 2 + int((yil-622) * 0.97023)
    # Hicri yılı Gregoryan yıla çevirme ve Ramazan Bayramı başlangıcı: Hicri yılın 10. ayının 1. günü
    baslangic_gregoryan_ramazan = Hijri(year, 10, 1)
    baslangic_gregoryan_ramazan = baslangic_gregoryan_ramazan.to_gregorian()
    ramazan_bayrami_baslangic = datetime.date(baslangic_gregoryan_ramazan.year, baslangic_gregoryan_ramazan.month,
                                              baslangic_gregoryan_ramazan.day)

    # Hicri yılı Gregoryan yıla çevirme ve Kurban Bayramı başlangıcı: Hicri yılın 12. ayının 10. günü
    baslangic_gregoryan_kurban = Hijri(year, 12, 10).to_gregorian()
    kurban_bayrami_baslangic = datetime.date(baslangic_gregoryan_kurban.year, baslangic_gregoryan_kurban.month,
                                             baslangic_gregoryan_kurban.day)

    hafta_ici_bayram_gunleri_sayisi = 0
    # Hafta içine denk gelen Ramazan Bayramı günlerini bulma
    ramazan_bayrami = ramazan_bayrami_baslangic
    for i in range(0, 2):
        if(ramazan_bayrami.weekday()+i) < 5:
            hafta_ici_bayram_gunleri_sayisi += 1

    # Hafta içine denk gelen Kurban Bayramı günlerini bulma
    kurban_bayrami = kurban_bayrami_baslangic
    for j in range(0, 3):
        if (kurban_bayrami.weekday()+j) < 5:
            hafta_ici_bayram_gunleri_sayisi += 1

    return hafta_ici_bayram_gunleri_sayisi


def total(gelecek_yil):
    # gelecek_yil = datetime.datetime.now().year + 1
    hafta_sonu_sayisi = hafta_sonu_gunleri(gelecek_yil)
    resmi_tatil_sayisi = hafta_ici_resmi_tatiller(gelecek_yil)
    bayram_gunleri_sayisi = hafta_ici_bayram_gunleri(gelecek_yil)

    kesin_gun = hafta_sonu_sayisi + resmi_tatil_sayisi + bayram_gunleri_sayisi
    return kesin_gun


def totalkayitlar():
    # Kayıtları yıl bazında toplama ve kaydetme
    cursor = conn.cursor()

    # Kayıt kontrolü
    cursor.execute("SELECT COUNT(*) FROM zarartahmini")
    record_count = cursor.fetchone()[0]

    if record_count > 0:
        # Yıl bazında toplama işlemini gerçekleştirme
        cursor.execute("SELECT Yil, SUM(TahminiZarar) FROM zarartahmini GROUP BY Yil")
        total_tahminler = cursor.fetchall()

        # Toplam tahminleri totaltahminler tablosuna kaydetme
        for tahmin in total_tahminler:
            yil, toplam_tahmin = tahmin

            # Kayıt kontrolü
            cursor.execute(
                "SELECT Id, Yil FROM totaltahminler WHERE Yil = %s",
                (yil,)
            )
            existing_record = cursor.fetchone()

            if existing_record:
                # Kayıt güncelleme
                record_id, created_date = existing_record
                update_query = "UPDATE totaltahminler SET TotalTahmin = %s, UpdatedDate = %s WHERE Id = %s"
                cursor.execute(update_query, (toplam_tahmin, datetime.datetime.now(), record_id))
            else:
                # Yeni kayıt oluşturma
                insert_query = "INSERT INTO totaltahminler (Yil, TotalTahmin, CalisilmayanGunler, " \
                               "CreatedDate, UpdatedDate) " \
                               "VALUES (%s, %s, %s, %s)"
                current_time = datetime.datetime.now()
                cursor.execute(insert_query, (yil, toplam_tahmin, total(yil), current_time, current_time))

            conn.commit()
    cursor.close()
    conn.close()
