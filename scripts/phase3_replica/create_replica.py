import sqlite3
from pymongo import MongoClient
import os
import shutil

#mongod --dbpath ./data/mongo/standalone/

def migrate_flat():
    # 1. Connexions
    # Remplace 'db.sqlite3' par le chemin réel vers ta base Django
    sqlite_conn.row_factory = sqlite3.Row  # Pour extraire les données sous forme de dict
    sqlite_cur = sqlite_conn.cursor()

    # Liste des tables à migrer (exemples à adapter selon tes modèles)
    tables = ['CHARACTER', 'DIRECTOR', 'EPISODE','GENRE', 'KNOWN_FOR', 'MOVIE','PERSON', 'PRINCIPAL', 'PROFESSION','RATING', 'TITLE', 'WRITER']

    for table in tables:
        print(f"Migration de la table : {table}...")

        # 2. Extraction des données (pas de CSV !)
        sqlite_cur.execute(f"SELECT * FROM {table}")
        rows = [dict(row) for row in sqlite_cur.fetchall()]

        if rows:
            # 3. Insertion dans MongoDB
            collection = db[table]
            # On nettoie la collection avant pour éviter les doublons si on relance le script
            collection.delete_many({}) 
            
            result = collection.insert_many(rows)
            
            # 4. Vérification des comptages
            mongo_count = collection.count_documents({})
            print(f"✅ {table} : {len(rows)} extraits -> {mongo_count} insérés.")
        else:
            print(f"⚠️ La table {table} est vide.")


    print("\nMigration terminée avec succès !")

if __name__ == "__main__":
    # Connexion au Replica Set (Ports 27017, 27018, 27019)
    try:
        mongo_uri = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
        mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        
        # Vérification de la connexion
        mongo_client.admin.command('ping')
        print("Connecté au Replica Set MongoDB avec succès !")
        
        db = mongo_client['MongoDB']

        # Connexion SQLite
        sqlite_conn = sqlite3.connect('./cineexplorer/data/imdb.db')

        migrate_flat()

        sqlite_conn.close()
        mongo_client.close()
        
    except Exception as e:
        print(f"Erreur de connexion : {e}")