#mongod --dbpath ./data/mongo/standalone/

import pandas as pd
import time
from pymongo import MongoClient, ASCENDING, DESCENDING

# Connexion à MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['MongoDB']

def Filmographie(name):
    pipeline = [
        # 1. Trouver l'acteur (WHERE name LIKE '...')
        {"$match": {"primaryName": {"$regex": f"^{name}", "$options": "i"}}},
        
        # 2. Joindre PRINCIPAL (ON Persons.PID = PRINCIPAL.PID)
        {"$lookup": {
            "from": "PRINCIPAL",
            "localField": "pid",
            "foreignField": "pid",
            "as": "roles"
        }},
        {"$unwind": "$roles"},
        
        # 3. Joindre Movies (ON PRINCIPAL.MID = MOVIE.MID)
        {"$lookup": {
            "from": "MOVIE",
            "localField": "roles.mid",
            "foreignField": "mid",
            "as": "MOVIE"
        }},
        {"$unwind": "$MOVIE"},
        
        # 4. Joindre Ratings (ON MOVIE.MID = RATING.MID)
        {"$lookup": {
            "from": "RATING",
            "localField": "MOVIE.mid",
            "foreignField": "mid",
            "as": "RATING"
        }},
        
        # Gestion du LEFT JOIN pour rating (peut être vide)
        {"$addFields": {
            "rating_val": {"$arrayElemAt": ["$rating.averageRating", 0]}
        }},

        # 5. Projection (SELECT ...)
        {"$project": {
            "_id": 0,
            "Titre": "$MOVIE.primaryTitle",
            "Année": "$MOVIE.startYear",
            "Note": "$rating_val"
        }},
        {"$sort": {"Année": -1}}
    ]

    # Exécution sur la collection PERSON
    data = list(db.PERSON.aggregate(pipeline))
    df = pd.DataFrame(data)
    print(df.head(5))

def Top_Film(genre_name, debut, fin, N):
    pipeline = [
        # 1. Filtre sur les années (Équivalent de WHERE m.startYear > ? AND m.endYear < ?)
        { "$match": { 
            "startYear": { "$gt": debut }, 
            "endYear": { "$lt": fin } 
        }},
        
        # 2. Jointure avec GENRE pour filtrer par le genre spécifié
        { "$lookup": {
            "from": "GENRE",
            "localField": "mid",
            "foreignField": "mid",
            "as": "genre_info"
        }},
        { "$unwind": "$genre_info" },
        
        # 3. Filtre sur le genre (Équivalent de g.genre = ?)
        { "$match": { "genre_info.genre": genre_name } },
        
        # 4. Jointure avec RATING (Équivalent de LEFT JOIN RATING)
        { "$lookup": {
            "from": "RATING",
            "localField": "mid",
            "foreignField": "mid",
            "as": "rating_info"
        }},
        { "$unwind": { "path": "$rating_info", "preserveNullAndEmptyArrays": True } },
        
        # 5. Sélection et renommage des champs (Équivalent du SELECT)
        { "$project": {
            "_id": 0,
            "Film": "$primaryTitle",
            "Année": "$startYear",
            "Note": "$rating_info.averageRating"
        }},
        
        # 6. Tri par note décroissante et limite (Équivalent de ORDER BY DESC LIMIT N)
        { "$sort": { "Note": -1 } },
        { "$limit": N }
    ]

    # Exécution sur la collection MOVIE
    data = list(db.MOVIE.aggregate(pipeline))
    df = pd.DataFrame(data)
    print(df.head(5))


def Multi_Role():
    pipeline = [
        # 1. Filtre initial : On exclut les noms de personnages nuls ou valant 'None'
        { "$match": { 
            "name": { "$ne": None, "$nin": ["None", "", "null"] } 
        }},
        
        # 2. Groupement par Acteur (pid) et Film (mid)
        # On utilise $addToSet pour obtenir les noms de personnages DISTINCTS
        { "$group": {
            "_id": { "pid": "$pid", "mid": "$mid" },
            "unique_roles": { "$addToSet": "$name" }
        }},
        
        # 3. Calcul du nombre de rôles (Équivalent du COUNT DISTINCT)
        { "$project": {
            "pid": "$_id.pid",
            "mid": "$_id.mid",
            "Nb_Roles": { "$size": "$unique_roles" },
            "_id": 0
        }},
        
        # 4. Équivalent du HAVING Nb_Roles > 1
        { "$match": { "Nb_Roles": { "$gt": 1 } }},
        
        # 5. Jointure avec PERSON pour obtenir le nom de l'acteur
        { "$lookup": {
            "from": "PERSON",
            "localField": "pid",
            "foreignField": "pid",
            "as": "actor_info"
        }},
        { "$unwind": "$actor_info" },
        
        # 6. Jointure avec MOVIE pour obtenir le titre du film
        { "$lookup": {
            "from": "MOVIE",
            "localField": "mid",
            "foreignField": "mid",
            "as": "movie_info"
        }},
        { "$unwind": "$movie_info" },
        
        # 7. Mise en forme finale (SELECT)
        { "$project": {
            "Acteur": "$actor_info.primaryName",
            "Film": "$movie_info.primaryTitle",
            "Nb_Roles": 1
        }},
        
        # 8. Tri final (ORDER BY)
        { "$sort": { "Nb_Roles": -1, "Acteur": 1 } }
    ]

    # On commence par la collection CHARACTER car c'est la base du groupement
    data = list(db.CHARACTER.aggregate(pipeline))
    df = pd.DataFrame(data)
    print(df.head(5))


def Collaboration(actor):
    # 1. Sous-requête : Récupérer les 'mid' des films où l'acteur a joué
    # On commence par PERSON pour trouver le pid de l'acteur, puis on cherche ses films dans CHARACTER
    actor_films_pipeline = [
        { "$match": { "primaryName": actor } },
        { "$lookup": {
            "from": "CHARACTER",
            "localField": "pid",
            "foreignField": "pid",
            "as": "roles"
        }},
        { "$unwind": "$roles" },
        { "$group": { "_id": "$roles.mid" } } # On ne garde que les mid uniques
    ]
    
    actor_mids_docs = list(db.PERSON.aggregate(actor_films_pipeline))
    mids = [doc['_id'] for doc in actor_mids_docs]

    # Si l'acteur n'a pas de films, on renvoie un DataFrame vide
    if not mids:
        return pd.DataFrame(columns=['Realisateur', 'Nombre_de_Films'])

    # 2. Requête principale : Compter les collaborations par réalisateur
    collaboration_pipeline = [
        # On filtre les réalisateurs travaillant sur ces films (WHERE mid IN ...)
        { "$match": { "mid": { "$in": mids } } },
        
        # Jointure avec PERSON pour obtenir le nom du réalisateur
        { "$lookup": {
            "from": "PERSON",
            "localField": "pid",
            "foreignField": "pid",
            "as": "dir_info"
        }},
        { "$unwind": "$dir_info" },
        
        # Groupement par réalisateur
        { "$group": {
            "_id": "$dir_info.primaryName",
            "Nombre_de_Films": { "$sum": 1 }
        }},
        
        # Mise en forme finale
        { "$project": {
            "_id": 0,
            "Realisateur": "$_id",
            "Nombre_de_Films": 1
        }},
        
        # Tri (ORDER BY Nombre_de_Films DESC, Realisateur ASC)
        { "$sort": { "Nombre_de_Films": -1, "Realisateur": 1 } }
    ]

    data = list(db.DIRECTOR.aggregate(collaboration_pipeline))
    df = pd.DataFrame(data)
    print(df.head(5))




def Genre_Populaire():
    pipeline = [
        # 1. Filtre initial : On ne garde que les longs-métrages (WHERE titleType = 'movie')
        { "$match": { "titleType": "movie" } },
        
        # 2. Jointure avec GENRE
        { "$lookup": {
            "from": "GENRE",
            "localField": "mid",
            "foreignField": "mid",
            "as": "genre_docs"
        }},
        { "$unwind": "$genre_docs" },
        
        # 3. Jointure avec RATING
        { "$lookup": {
            "from": "RATING",
            "localField": "mid",
            "foreignField": "mid",
            "as": "rating_docs"
        }},
        { "$unwind": "$rating_docs" },
        
        # 4. Groupement par Genre et calculs (GROUP BY g.genre)
        { "$group": {
            "_id": "$genre_docs.genre",
            "Note_Moyenne_Brute": { "$avg": "$rating_docs.averageRating" },
            "Nombre_de_Films": { "$sum": 1 }
        }},
        
        # 5. Filtre post-agrégation (HAVING Note_Moyenne > 7.0 AND Nombre_de_Films > 50)
        { "$match": {
            "Note_Moyenne_Brute": { "$gt": 7.0 },
            "Nombre_de_Films": { "$gt": 50 }
        }},
        
        # 6. Mise en forme et arrondi (SELECT ROUND(..., 2))
        { "$project": {
            "_id": 0,
            "Genre": "$_id",
            "Note_Moyenne": { "$round": ["$Note_Moyenne_Brute", 2] },
            "Nombre_de_Films": 1
        }},
        
        # 7. Tri (ORDER BY Note_Moyenne DESC)
        { "$sort": { "Note_Moyenne": -1 } }
    ]

    # On commence par la collection MOVIE car le filtre initial (titleType) est dedans
    data = list(db.MOVIE.aggregate(pipeline))
    df = pd.DataFrame(data)
    print(df.head(5))


def Evolution_Carriere(acteur_nom):
    pipeline = [
        # 1. Trouver l'acteur (Filtre initial sur PERSON)
        { "$match": { "primaryName": acteur_nom } },
        
        # 2. Joindre ses rôles (CHARACTER)
        { "$lookup": {
            "from": "CHARACTER",
            "localField": "pid",
            "foreignField": "pid",
            "as": "roles"
        }},
        { "$unwind": "$roles" },
        
        # 3. Joindre les films (MOVIE) avec filtres sur le type et l'année
        { "$lookup": {
            "from": "MOVIE",
            "localField": "roles.mid",
            "foreignField": "mid",
            "as": "movie"
        }},
        { "$unwind": "$movie" },
        { "$match": { 
            "movie.titleType": "movie", 
            "movie.startYear": { "$ne": None } 
        }},
        
        # 4. Joindre les notes (RATING)
        { "$lookup": {
            "from": "RATING",
            "localField": "roles.mid",
            "foreignField": "mid",
            "as": "rating"
        }},
        { "$unwind": "$rating" },
        
        # 5. Calcul de la décennie (Équivalent de la CTE FilmsDecennies)
        { "$addFields": {
            "Decennie": { 
                "$multiply": [
                    { "$floor": { "$divide": ["$movie.startYear", 10] } },
                    10
                ]
            }
        }},
        
        # 6. Groupement par décennie (GROUP BY Decennie)
        { "$group": {
            "_id": "$Decennie",
            "Nombre_Films": { "$sum": 1 },
            "Note_Moyenne": { "$avg": "$rating.averageRating" }
        }},
        
        # 7. Mise en forme finale et arrondi
        { "$project": {
            "_id": 0,
            "Periode": "$_id",
            "Nombre_Films": 1,
            "Note": { "$round": ["$Note_Moyenne", 2] }
        }},
        
        # 8. Tri par période croissante
        { "$sort": { "Periode": 1 } }
    ]

    data = list(db.PERSON.aggregate(pipeline))
    df = pd.DataFrame(data)
    print(df.head(5))


def Meilleur_Film_Par_Genre():
    pipeline = [
        # 1. Filtre initial sur les films populaires
        { "$match": { "titleType": "movie" } },
        
        # 2. Jointure avec RATING pour filtrer par votes
        { "$lookup": {
            "from": "RATING",
            "localField": "mid",
            "foreignField": "mid",
            "as": "rating"
        }},
        { "$unwind": "$rating" },
        { "$match": { "rating.numVotes": { "$gt": 10000 } }},
        
        # 3. Jointure avec GENRE
        { "$lookup": {
            "from": "GENRE",
            "localField": "mid",
            "foreignField": "mid",
            "as": "genre_doc"
        }},
        { "$unwind": "$genre_doc" },
        
        # 4. CLASSEMENT (Équivalent du RANK() OVER PARTITION BY)
        { "$setWindowFields": {
            "partitionBy": "$genre_doc.genre",
            "sortBy": { "rating.averageRating": -1 }, # Tri par note descendante
            "output": {
                "Rang": { "$rank": {} }
            }
        }},
        
        # 5. Filtre sur les 3 meilleurs (Équivalent du WHERE Rang <= 3)
        { "$match": { "Rang": { "$lte": 3 } }},
        
        # 6. Sélection finale (Projection)
        { "$project": {
            "_id": 0,
            "Genre": "$genre_doc.genre",
            "Film": "$primaryTitle",
            "Note": "$rating.averageRating",
            "Rang": 1
        }},
        
        # 7. Tri final pour l'affichage
        { "$sort": { "Genre": 1, "Rang": 1 } }
    ]

    data = list(db.MOVIE.aggregate(pipeline))
    df = pd.DataFrame(data)
    print(df.head(5))



def Carriere_Propulse():
    pipeline = [
        # 1. Filtre initial sur les longs-métrages
        { "$match": { "titleType": "movie" } },
        
        # 2. Jointures pour obtenir Acteurs, Films et Notes
        { "$lookup": {
            "from": "RATING",
            "localField": "mid",
            "foreignField": "mid",
            "as": "r"
        }},
        { "$unwind": "$r" },
        { "$lookup": {
            "from": "CHARACTER",
            "localField": "mid",
            "foreignField": "mid",
            "as": "c"
        }},
        { "$unwind": "$c" },
        { "$lookup": {
            "from": "PERSON",
            "localField": "c.pid",
            "foreignField": "pid",
            "as": "p"
        }},
        { "$unwind": "$p" },

        # 3. Équivalent du premier ROW_NUMBER() : Numéroter tous les films de la carrière
        { "$setWindowFields": {
            "partitionBy": "$p.pid",
            "sortBy": { "startYear": 1 },
            "output": {
                "FilmNum": { "$documentNumber": {} }
            }
        }},

        # 4. Identifier les films à succès (> 200 000 votes)
        # On utilise une condition pour marquer les succès sans filtrer les autres documents immédiatement
        { "$addFields": {
            "isSuccess": { "$gt": ["$r.numVotes", 200000] }
        }},

        # 5. Équivalent du second ROW_NUMBER() : Numéroter seulement les succès chronologiquement
        { "$setWindowFields": {
            "partitionBy": "$p.pid",
            "sortBy": { "startYear": 1 },
            "output": {
                "PremierSucces": {
                    "$sum": { "$cond": ["$isSuccess", 1, 0] }
                }
            }
        }},
        
        # 6. Logique du Breakthrough :
        # - On veut que le film actuel soit un succès (isSuccess: True)
        # - On veut que ce soit le TOUT PREMIER succès de l'acteur (SuccessRank cumulé == 1)
        # - On veut que ce ne soit PAS son premier film en carrière (FilmNum > 1)
        { "$match": { 
            "isSuccess": True,
            "FilmNum": { "$gt": 1 }
        }},
        
        # On re-vérifie que c'est bien le premier succès rencontré chronologiquement
        { "$setWindowFields": {
            "partitionBy": "$p.pid",
            "sortBy": { "startYear": 1 },
            "output": {
                "OrdreSucces": { "$documentNumber": {} }
            }
        }},
        { "$match": { "OrdreSucces": 1 } },

        # 7. Projection et Tri final
        { "$project": {
            "_id": 0,
            "Nom": "$p.primaryName",
            "Film": "$primaryTitle"
        }},
        { "$sort": { "Nom": 1 } }
    ]

    # Utilisation de allowDiskUse car les Window Functions sur de gros volumes sont gourmandes en RAM
    data = list(db.MOVIE.aggregate(pipeline, allowDiskUse=True))
    df = pd.DataFrame(data)
    print(df.head(5))


def Derniere():
    pipeline = [
        # 1. Filtre sur les films notés > 9 et de type 'movie'
        { "$match": { "titleType": "movie" } },
        
        # 2. Jointure avec RATING (obligatoire pour filtrer sur la note)
        { "$lookup": {
            "from": "RATING",
            "localField": "mid",
            "foreignField": "mid",
            "as": "rating_info"
        }},
        { "$unwind": "$rating_info" },
        { "$match": { "rating_info.averageRating": { "$gt": 9.0 } } },

        # 3. Jointure avec PRINCIPAL pour compter les personnes
        { "$lookup": {
            "from": "PRINCIPAL",
            "localField": "mid",
            "foreignField": "mid",
            "as": "participants"
        }},
        
        # 4. Comptage des participants (COUNT DISTINCT pr.pid)
        { "$project": {
            "_id": 0,
            "Film": "$primaryTitle",
            "Note": "$rating_info.averageRating",
            # On utilise $size sur l'array participants pour obtenir le compte
            "Nombre_Total_Personnes": { "$size": "$participants" }
        }},

        # 5. Tri par note et limite à 3
        { "$sort": { "Note": -1 } },
        { "$limit": 3 }
    ]

    # Exécution
    data = list(db.MOVIE.aggregate(pipeline))
    df = pd.DataFrame(data)
    print(df.head(5))









# --- Gestion des Index MongoDB ---

def Fin_Index():
    collections = ['movies', 'persons', 'characters', 'ratings', 'genres', 'directors', 'principals']
    for col in collections:
        db[col].drop_indexes()
    print("Tous les index ont été supprimés.")

def Create_Indexes():
    db.persons.create_index([("primaryName", ASCENDING)])
    db.characters.create_index([("pid", ASCENDING)])
    db.characters.create_index([("mid", ASCENDING)])
    db.movies.create_index([("startYear", ASCENDING)])
    db.ratings.create_index([("averageRating", DESCENDING)])
    db.genres.create_index([("genre", ASCENDING)])
    print("Index MongoDB créés.")


# --- Exécution du Benchmark ---

# Connexion à MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['MongoDB'] # Assure-toi que c'est le même nom partout

def get_db_size():
    # Récupère la taille des données + index en Mo
    stats = db.command("dbStats")
    return stats['storageSize'] / (1024 * 1024)

# --- Garde tes fonctions Filmographie, Top_Film, etc. telles quelles ---
# (Elles sont correctes suite à nos échanges précédents)

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
        results[func.__name__] = round((e - s) * 1000, 2)
        
    return results

# --- Exécution du Benchmark corrigée ---

# 1. Nettoyage et Mesure Sans Index
Fin_Index()
# On attend un peu que MongoDB traite la suppression des index
time.sleep(2) 
size_before = get_db_size()

print("Calcul des temps sans index...")
temps_sans = run_benchmarks_detailed()

# 2. Création des Index
print("Création des index...")
Create_Indexes()

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
    gain = round(((t_sans - t_avec) / t_sans) * 100, 2) if t_sans > 0 else 0
    print(f"{nom:<25} | {t_sans:<15} | {t_avec:<15} | {gain:<10}%")

print("="*85)
print(f"Taille de la base : {round(size_before, 2)} Mo (Sans) -> {round(size_after, 2)} Mo (Avec)")
