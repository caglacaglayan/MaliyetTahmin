import pandas as pd
from sklearn.linear_model import LinearRegression
from sqlalchemy import create_engine
import psycopg2
import datetime


def tahmin():
    # Veritabanına bağlantı kurma
    global cursor
    db_username = 'postgres'
    db_password = 'admin'
    db_host = 'localhost'
    db_port = '5432'
    db_name = 'shepherd'

    db_url = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(db_url)

    conn = psycopg2.connect(
        database="shepherd",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

    # Veritabanından veri okuma
    query = "SELECT * FROM maliyetzarari"
    df = pd.read_sql_query(query, engine)

    if not df.empty:
        # Veriyi işleme ve model eğitme
        X = df[['PersonID', 'Yil']]
        y = df['DevamsizGun']
        model = LinearRegression()
        model.fit(X, y)

        # Devamsızlık tahminleri
        calisanlar = df['PersonID'].unique()
        yillar = df['Yil'].unique()

        for calisanid in calisanlar:
            for yil in yillar:
                # Tahmin yapma
                tahmin = model.predict([[calisanid, yil]])[0]

                # Kayıt kontrolü
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM zarartahmini WHERE PersonID = %s AND Yil = %s",
                    (calisanid, yil)
                )
                existing_record = cursor.fetchone()

                if existing_record:
                    # Kayıt güncelleme
                    record_id, created_date = existing_record
                    update_query = "UPDATE zarartahmini SET TahminiZarar = %s, UpdatedDate = %s WHERE Id = %s"
                    cursor.execute(update_query, (tahmin, datetime.datetime.now(), record_id))
                else:
                    # Yeni kayıt oluşturma
                    insert_query = "INSERT INTO zarartahmini (PersonID, Yil, TahminiZarar, CreatedDate, UpdatedDate) " \
                                   "VALUES (%s, %s, %s, %s, %s)"
                    cursor.execute(insert_query,
                                   (calisanid, yil, tahmin, datetime.datetime.now(), datetime.datetime.now()))

                conn.commit()

    cursor.close()
    conn.close()
    engine.dispose()
