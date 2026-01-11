import pandas as pd
import sqlite3
import time
import os


conn = sqlite3.connect('./cineexplorer/data/imdb.db')

def Filmographie(name):

    query = """
        SELECT 
            m.primaryTitle as Film, 
            m.startYear as Année,
            c.name as Personnage,
            r.averageRating as Note
        FROM CHARACTER c
        JOIN PERSON p ON c.pid = p.pid
        JOIN MOVIE m ON c.mid = m.mid
        LEFT JOIN RATING r ON m.mid = r.mid
        WHERE p.primaryName = ?
        ORDER BY m.startYear DESC;
    """

    df = pd.read_sql_query(query, conn, params=(name,))
    #print(df.head(20))


def Top_Film(genre, debut, fin, N):

    query = """
        SELECT 
            m.primaryTitle as Film, 
            m.startYear as Année,
            r.averageRating as Note
        FROM MOVIE m
        LEFT JOIN GENRE g on m.mid = g.mid
        LEFT JOIN RATING r ON m.mid = r.mid
        WHERE (g.genre = ? and (m.startYear > ? and m.endYear < ?))
        ORDER BY r.averageRating DESC
        LIMIT ?;
    """

    df = pd.read_sql_query(query, conn, params=(genre, debut, fin, N,))
    #print(df.head(20))


def Multi_Role():
    query = """
        SELECT 
            p.primaryName as Acteur, 
            m.primaryTitle as Film, 
            COUNT(DISTINCT c.name) as Nb_Roles
        FROM CHARACTER c
        JOIN PERSON p ON c.pid = p.pid
        JOIN MOVIE m ON c.mid = m.mid
        WHERE c.name IS NOT NULL AND c.name != 'None'
        GROUP BY p.pid, m.mid
        HAVING Nb_Roles > 1
        ORDER BY Nb_Roles DESC, Acteur ASC;
    """

    df = pd.read_sql_query(query, conn)
    #print(df.head(20))


def Collaboration(actor):
    query = """
        SELECT 
        p_dir.primaryName AS Realisateur,
        COUNT(d.mid) AS Nombre_de_Films
    FROM DIRECTOR d
    JOIN PERSON p_dir ON d.pid = p_dir.pid
    JOIN MOVIE m ON d.mid = m.mid
    WHERE d.mid IN (
        -- Sous-requête : IDs des films de l'acteur spécifique
        SELECT c.mid 
        FROM CHARACTER c
        JOIN PERSON p_act ON c.pid = p_act.pid
        WHERE p_act.primaryName = ?
    )
    GROUP BY p_dir.pid
    ORDER BY Nombre_de_Films DESC, Realisateur ASC;
    """

    df = pd.read_sql_query(query, conn, params=(actor,))
    #print(df.head(20))


def Genre_Populaire():
    query = """
    SELECT 
        g.genre as Genre, 
        ROUND(AVG(r.averageRating), 2) as Note_Moyenne,
        COUNT(m.mid) as Nombre_de_Films
    FROM GENRE g
    JOIN MOVIE m ON g.mid = m.mid
    JOIN RATING r ON m.mid = r.mid
    WHERE m.titleType = 'movie'
    GROUP BY g.genre
    HAVING Note_Moyenne > 7.0
       AND Nombre_de_Films > 50
    ORDER BY Note_Moyenne DESC;
    """

    df = pd.read_sql_query(query, conn)
    #print(df.head(20))



def Evolution_Carriere(acteur_nom):
    query = """
    WITH FilmsDecennies AS (
        -- On prépare les données : calcul de la décennie
        SELECT 
            m.startYear,
            (m.startYear / 10) * 10 AS Decennie,
            r.averageRating
        FROM PERSON p
        JOIN CHARACTER c ON p.pid = c.pid
        JOIN MOVIE m ON c.mid = m.mid
        JOIN RATING r ON m.mid = r.mid
        WHERE p.primaryName = ? 
          AND m.startYear IS NOT NULL
          AND m.titleType = 'movie'
    )
    SELECT 
        Decennie AS Periode,
        COUNT(*) AS Nombre_Films,
        ROUND(AVG(averageRating), 2) AS Note
    FROM FilmsDecennies
    GROUP BY Decennie
    ORDER BY Decennie ASC;
    """
    
    df = pd.read_sql_query(query, conn, params=(acteur_nom,))
    #print(df.head(20))


def Meilleur_Film_Par_Genre():
    query = """
    WITH ClassementFilms AS (
        SELECT 
            g.genre AS Genre,
            m.primaryTitle AS Film,
            r.averageRating AS Note,
            RANK() OVER (
                PARTITION BY g.genre 
                ORDER BY r.averageRating DESC, r.numVotes DESC
            ) as Rang
        FROM GENRE g
        JOIN MOVIE m ON g.mid = m.mid
        JOIN RATING r ON m.mid = r.mid
        WHERE m.titleType = 'movie' 
          AND r.numVotes > 10000
    )
    SELECT * FROM ClassementFilms
    WHERE Rang <= 3
    ORDER BY Genre ASC, Rang ASC;
    """
    
    df = pd.read_sql_query(query, conn)
    #print(df.head(20))


def Carriere_Propulse():
    query ="""
        WITH Carriere AS (
        SELECT 
            p.pid,
            p.primaryName as Nom,
            m.primaryTitle as Film,
            m.startYear,
            r.numVotes,
            ROW_NUMBER() OVER (PARTITION BY p.pid ORDER BY m.startYear ASC) as FilmNum
        FROM PERSON p
        JOIN CHARACTER c ON p.pid = c.pid
        JOIN MOVIE m ON c.mid = m.mid
        JOIN RATING r ON m.mid = r.mid
        WHERE m.titleType = 'movie'
    ),
    Breakthrough AS (
        SELECT pid, Nom, Film, startYear, FilmNum
        FROM (
            SELECT *, 
                ROW_NUMBER() OVER (PARTITION BY pid ORDER BY startYear ASC) as PremierSucces
            FROM Carriere 
            WHERE numVotes > 200000
        ) 
        WHERE PremierSucces = 1
    )
    SELECT 
        b.Nom, 
        b.Film
    FROM Breakthrough b
    WHERE b.FilmNum > 1 
    AND NOT EXISTS (
        SELECT 1 FROM Carriere c2 
        WHERE c2.pid = b.pid 
        AND c2.startYear < b.startYear 
        AND c2.numVotes > 200000
    )
    ORDER BY b.Nom ASC;
    """
    
    df = pd.read_sql_query(query, conn)
    #print(df.head(20))


#Les 3 films notés > 9 avec le nombre d'acteurs ayant participé.
def Derniere():
    query ="""
        EXPLAIN QUERY PLAN SELECT 
            m.primaryTitle AS Film,
            r.averageRating AS Note,
            COUNT(DISTINCT pr.pid) AS Nombre_Total_Personnes
        FROM MOVIE m
        JOIN RATING r ON m.mid = r.mid
        JOIN PRINCIPAL pr ON m.mid = pr.mid
        WHERE r.averageRating > 9.0 and
            m.titleType = 'movie'
        GROUP BY m.mid
        ORDER BY r.averageRating DESC
        LIMIT 3;
    """
    df = pd.read_sql_query(query, conn)
    #print(df.head(20))
    



DB_PATH = './cineexplorer/data/imdb.db'
conn = sqlite3.connect(DB_PATH)

def get_db_size():
    return os.path.getsize(DB_PATH) / (1024 * 1024)  # Taille en Mo


def Fin_Index():
    conn = sqlite3.connect('./cineexplorer/data/imdb.db')
    cursor = conn.cursor()

    # Récupérer tous les noms d'index créés par l'utilisateur
    cursor.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex%';")
    indexes = cursor.fetchall()

    for idx in indexes:
        index_name = idx[0]
        print(f"Suppression de l'index : {index_name}")
        cursor.execute(f"DROP INDEX IF EXISTS {index_name};")

    conn.commit()
    print("Tous les index personnalisés ont été supprimés.")



# --- Vos fonctions de requêtes restent identiques ---
# (Pensez juste à enlever "EXPLAIN QUERY PLAN" dans la fonction Derniere 
# pour mesurer le temps réel d'exécution)

def run_benchmarks_detailed():
    results = {}
    tasks = [
        (Filmographie, ["Tom Hanks"]),
        (Top_Film, ["Comedy", 1980, 2000, 10]),
        (Multi_Role, []),
        (Collaboration, ["Tom Hanks"]),
        (Genre_Populaire, []),
        (Evolution_Carriere, ["Clint Eastwood"]),
        (Meilleur_Film_Par_Genre, []),
        (Carriere_Propulse, []),
        (Derniere, [])
    ]
    
    for func, args in tasks:
        s = time.time()
        func(*args) if args else func()
        e = time.time()
        # On stocke le résultat en millisecondes
        results[func.__name__] = round((e - s) * 1000, 2)
        
    return results

# --- Exécution du Benchmark ---

# 1. Nettoyage et Mesure Sans Index
Fin_Index()
conn.execute("VACUUM") # Pour réinitialiser la taille réelle du fichier
size_before = get_db_size()
print("Calcul des temps sans index...")
temps_sans = run_benchmarks_detailed()

# 2. Création des Index
print("Création des index...")
conn.execute("CREATE INDEX IF NOT EXISTS idx_person_name ON PERSON(primaryName);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_char_pid ON CHARACTER(pid);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_char_mid ON CHARACTER(mid);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_principals_mid ON PRINCIPAL(mid);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_movie_year ON MOVIE(startYear);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_rating_score ON RATING(averageRating);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_genre_name ON GENRE(genre);")
conn.commit()

# 3. Mesure Avec Index
size_after = get_db_size()
print("Calcul des temps avec index...")
temps_avec = run_benchmarks_detailed()

# --- 4. Affichage du Tableau Final ---

print("\n" + "="*85)
print(f"{'Requêtes':<25} | {'Sans index (ms)':<15} | {'Avec index (ms)':<15} | {'Gain (%)':<10}")
print("-" * 85)

for nom in temps_sans.keys():
    t_sans = temps_sans[nom]
    t_avec = temps_avec[nom]
    
    # Calcul du gain en pourcentage
    # Si t_avec est proche de 0, le gain tend vers 100%
    gain = round(((t_sans - t_avec) / t_sans) * 100, 2) if t_sans > 0 else 0
    
    print(f"{nom:<25} | {t_sans:<15} | {t_avec:<15} | {gain:<10}%")

print("="*85)
print(f"Taille de la base : {round(size_before, 2)} Mo (Sans) -> {round(size_after, 2)} Mo (Avec)")