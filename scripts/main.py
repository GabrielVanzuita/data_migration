import os
from scripts.classes.connection_mongo import MongoConnection

# Usar variáveis de ambiente para maior segurança
username = os.getenv("MONGO_USERNAME", "gbrvbusiness")
password = os.getenv("MONGO_PASSWORD", "9AHmX4Hna0nvuANa")
clustername = os.getenv("MONGO_CLUSTERNAME", "Mongo_Studies")

mg_Con = MongoConnection (username=username, password=password, clustername=clustername)
direct_con = MongoConnection.connection()








