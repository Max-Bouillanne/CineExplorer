import sqlite3
import pandas as pd
import time
import os


DATABASE_FILE = './cineexplorer/data/imdb.db' 


def create_base():
    if os.path.exists(DATABASE_FILE):
        os.remove(DATABASE_FILE)


    print(f"Création de la base de données : {DATABASE_FILE}...")
    
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    # Activation des contraintes de clés étrangères (Indispensable pour SQLite)
    cursor.execute("PRAGMA foreign_keys = ON;")

    # =========================================================================
    # 1. TABLES PRINCIPALES (ENTITÉS)
    # Ces tables doivent exister pour que MID et PID puissent être référencés
    # =========================================================================

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS MOVIE (
        mid VARCHAR(50) PRIMARY KEY,
        titleType VARCHAR(50),
        primaryTitle VARCHAR(255),
        originalTitle VARCHAR(255),
        isAdult INTEGER,
        startYear INTEGER,
        endYear FLOAT,
        runtimeMinutes FLOAT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS PERSON (
        pid VARCHAR(50) PRIMARY KEY,
        primaryName VARCHAR(255),
        birthYear FLOAT,
        deathYear FLOAT
    );
    """)

    # --- ENTITÉS DÉPENDANTES (1-1 ou 1-N) ---

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS GENRE (
        mid VARCHAR(50),
        genre VARCHAR(50),
        PRIMARY KEY (mid, genre),
        FOREIGN KEY (mid) REFERENCES MOVIE(mid)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS RATING (
        mid VARCHAR(50) PRIMARY KEY,
        averageRating FLOAT,
        numVotes INTEGER,
        FOREIGN KEY (mid) REFERENCES MOVIE(mid)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS EPISODE (
        mid VARCHAR(50) PRIMARY KEY,
        parentMid VARCHAR(50),
        seasonNumber FLOAT,
        episodeNumber FLOAT,
        FOREIGN KEY (parentMid) REFERENCES MOVIE(mid)
    );
    """)

    # --- TABLES DE JONCTION (N-M) ---

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS PRINCIPAL (
        mid VARCHAR(50),
        ordering INTEGER,
        pid VARCHAR(50),
        category VARCHAR(100),
        job TEXT,
        PRIMARY KEY (mid, ordering),
        FOREIGN KEY (mid) REFERENCES MOVIE(mid),
        FOREIGN KEY (pid) REFERENCES PERSON(pid)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS CHARACTER (
        mid VARCHAR(50) NOT NULL,
        pid VARCHAR(50) NOT NULL,
        name TEXT,
        PRIMARY KEY (mid, pid, name),
        FOREIGN KEY (mid) REFERENCES MOVIE(mid),
        FOREIGN KEY (pid) REFERENCES PERSON(pid)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS DIRECTOR (
        mid VARCHAR(50),
        pid VARCHAR(50),
        PRIMARY KEY (mid, pid),
        FOREIGN KEY (mid) REFERENCES MOVIE(mid),
        FOREIGN KEY (pid) REFERENCES PERSON(pid)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS WRITER (
        mid VARCHAR(50),
        pid VARCHAR(50),
        PRIMARY KEY (mid, pid),
        FOREIGN KEY (mid) REFERENCES MOVIE(mid),
        FOREIGN KEY (pid) REFERENCES PERSON(pid)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS PROFESSION (
        pid VARCHAR(50),
        jobName VARCHAR(100),
        PRIMARY KEY (pid, jobName),
        FOREIGN KEY (pid) REFERENCES PERSON(pid)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS KNOWN_FOR (
        pid VARCHAR(50),
        mid VARCHAR(50),
        PRIMARY KEY (pid, mid),
        FOREIGN KEY (pid) REFERENCES PERSON(pid),
        FOREIGN KEY (mid) REFERENCES MOVIE(mid)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS TITLE (
        mid VARCHAR(50),
        ordering INTEGER,
        title TEXT,
        region VARCHAR(10),
        language VARCHAR(10),
        types TEXT,
        attributes TEXT,
        isOriginalTitle INTEGER,
        PRIMARY KEY (mid, ordering),
        FOREIGN KEY (mid) REFERENCES MOVIE(mid)
    );
    """)

    # Sauvegarde et fermeture
    conn.commit()
    conn.close()
    print('Création des tables réussis')


create_base()




def Import_Data(conn, csv_name, table_name, chunk_size=10000):
    global orphelin_supprime
    global ligne_ajoute

    print(f"\n-> Début de l'import de {csv_name}.csv dans la table {table_name}...")
    csv_path = f'./cineexplorer/data/csv/{csv_name}.csv'
    
    start_time = time.time()

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False):
        
        #Renome les colonnes
        chunk.columns = [col[2:-3] for col in chunk.columns]

        #Vérifie que les pids / mids existent
        try:
            len_chunk = len(chunk)
            chunk = chunk[chunk['mid'].isin(valid_mids)]
            orphelin_supprime += len_chunk - len(chunk)
        except:
            pass

        try:
            len_chunk = len(chunk)
            chunk = chunk[chunk['pid'].isin(valid_pids)]
            orphelin_supprime += len_chunk - len(chunk)
        except:
            pass

        try:
            len_chunk = len(chunk)
            chunk = chunk[chunk['parentMid'].isin(valid_mids)]
            orphelin_supprime += len_chunk - len(chunk)
        except:
            pass

        # Remplacement des valeurs NaN par None pour SQL
        chunk = chunk.where(pd.notnull(chunk), None)

        #Enleve les dupliqués
        chunk = chunk.drop_duplicates(subset=chunk.columns)


        chunk.to_sql(table_name, conn, if_exists='append', index=False)
        ligne_ajoute += len(chunk)

    end_time = time.time()

        


tables = {
    'movies': 'MOVIE',   # Mid
    'persons': 'PERSON', # Pid

    'ratings': 'RATING',
    'genres': "GENRE",
    'episodes': 'EPISODE',

    'principals': 'PRINCIPAL',
    'characters': 'CHARACTER',
    'directors': 'DIRECTOR',
    'writers': 'WRITER',
    'professions': 'PROFESSION',

    'knownformovies': 'KNOWN_FOR',
    'titles': 'TITLE',
}

#Extrait les mids du fichier csv movies
valid_mids = set(pd.read_csv('./cineexplorer/data/csv/movies.csv', usecols=[0]).iloc[:, 0])

#Extrait les pids du fichier csv persons
valid_pids = set(pd.read_csv('./cineexplorer/data/csv/persons.csv', usecols=[0]).iloc[:, 0])


orphelin_supprime = 0
ligne_ajoute = 0
conn = sqlite3.connect(DATABASE_FILE)
for key, values in tables.items():
    Import_Data(conn, key, values)


print(f"Nombre d'orphelin supprimés : {orphelin_supprime}")
print(f"Nombre de lignes ajoutées : {ligne_ajoute}")
conn.close()