import psycopg2

## Bu değeri localinde çalışırken kendi passwordün yap. Ama kodu pushlarken 'postgres' olarak bırak.
password = 'postgres'

def connect_db():
    conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password=password)
    return conn

# DATE_TRUNC ile ay bazlı kayıt sayılarını listele
def question_1_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("""select date_trunc('month', enrollment_date) as month, count(*) as enrollment_count
                   from enrollments
                   group by date_trunc('month', enrollment_date)
                   order by month;""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


# DATE_PART ile sadece kayıtların yıl bilgisini al
def question_2_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("""select date_part('year',enrollment_date) as year
                    from enrollments;""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


# Tüm öğrencilerin yaşlarının toplamını dönen bir sql sorgusu yaz.
def question_3_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("""select sum(age) as total_age
                   from students; """)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


# Tüm kurs sayısını bul
def question_4_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("""select count(*) as course_count
                   from courses; """)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


# Yaşı ortalama yaştan büyük olan öğrencileri getir
def question_5_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("""select avg(age) as avg_age
                   from students; """)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


# Her kursun en eski kayıt tarihini bul
def question_6_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("""select c.course_name, min(e.enrollment_date) as oldest_enrollment
                   from courses as c
                   join enrollments as e
                   on c.course_id=e.course_id
                   group by course_name; """)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


# Her kurs için öğrencilerin ortalama yaşlarını bulun. 
# Sorgu course_name ve ortalama yaş(avg_age) değerlerini dönmelidir.
def question_7_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("""select course_name, avg(s.age) as avg_age
                   from courses as c
                   join enrollments as e 
                   on c.course_id = e.course_id
                   join students as s 
                   on e.student_id = s.student_id
                   group by c.course_name; """)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


# En genç öğrencinin yaşını getiren sorguyu yazınız.
def question_8_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("""select min(age) as youngest_age
                   from students; """)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# Her derse kayıt olmuş öğrenci sayısını bulunuz.
def question_9_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("""select c.course_name, count(e.student_id) as student_count
                   from courses as c
                   left join enrollments as e 
                   on c.course_id = e.course_id
                   group by c.course_name;""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data


#Tüm kayıt olunmuş derslerin sadece isimlerini getirinz.
def question_10_query():
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute("""select distinct c.course_name
                   from courses as c
                   join enrollments as e 
                   on c.course_id = e.course_id;""")
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data
