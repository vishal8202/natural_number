import mysql.connector
from kafka import KafkaConsumer

try:
    mydb = mysql.connector.connect(host = 'localhost', user = 'root', password = '',database = 'kafka_db')
except mysql.connector.Error as e:
    print("MySql error",e)

mycursor = mydb.cursor()

bootstrap_server = ["localhost:9092"]

topic = "naturalNumber"

consumer = KafkaConsumer(topic, bootstrap_servers = bootstrap_server)

for i in consumer:
    print(str(i.value.decode()))
    reverse_number = (i.value.decode()[::-1])
   
    sql = "INSERT INTO `reverse`(`revnumber`) VALUES (%s)"
    data = (reverse_number,)
    mycursor.execute(sql,data)
    mydb.commit()
    print("Reverse number added to db ", reverse_number)