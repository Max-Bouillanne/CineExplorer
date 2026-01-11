import pymongo
import time
from pymongo import UpdateOne
import json


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["MongoDB"]

def parse_char(c):
    if not c or c == 'None': return []
    try:
        # Nettoie les guillemets simples pour le format JSON si nécessaire
        return json.loads(c.replace("'", '"'))
    except:
        return [c] # Retourne tel quel si ce n'est pas du JSON


def migrate_in_batches(batch_size=10000):
    print("Vérification des index...")
    db["PRINCIPAL"].create_index("mid")
    db["RATING"].create_index("mid")
    db["GENRE"].create_index("mid")
    db["PERSON"].create_index("pid")

    total_movies = db["MOVIE"].count_documents({})
    cursor = db["MOVIE"].find({})
    
    batch = []
    processed = 0
    start_time = time.time()

    print(f"Début de la migration de {total_movies} films...")

    for movie in cursor:
        mid = movie["mid"]
        
        # 1. Récupération des données liées (requêtes ciblées)
        rating = db["RATING"].find_one({"mid": mid}, {"_id": 0, "averageRating": 1, "numVotes": 1})
        genres = list(db["GENRE"].find({"mid": mid}, {"_id": 0, "genre": 1}))
        principals = list(db["PRINCIPAL"].find({"mid": mid}))
        
        # 2. Récupération des noms des personnes impliquées
        pids = [p["pid"] for p in principals]
        persons = {p["pid"]: p["primaryName"] for p in db["PERSON"].find({"pid": {"$in": pids}}, {"pid": 1, "primaryName": 1})}



        # 3. Construction du document dénormalisé
        movie_complete = {
            "_id": mid,
            "title": movie.get("primaryTitle"),
            "year": movie.get("startYear"),
            "runtime": movie.get("runtimeMinutes"),
            "genres": [g["genre"] for g in genres],
            "rating": {
                "average": rating["averageRating"] if rating else None,
                "votes": rating["numVotes"] if rating else None
            },
            # DANS VOTRE BOUCLE DE MIGRATION, modifiez la partie 'cast' :
            "cast": [
                {
                    "person_id": p.get("pid"),
                    "name": persons.get(p["pid"], "Unknown"),
                    "characters": parse_char(p.get("characters")), # TRANSFORMATION ICI
                    "ordering": p.get("ordering")
                } for p in principals if p["category"] in ["actor", "actress"]
            ],
            "directors": [
                {
                    "name": persons.get(p["pid"], "Unknown")
                } for p in principals if p["category"] == "director"
            ]
        }

        # Utilisation de ReplaceOne pour permettre la reprise en cas d'erreur
        batch.append(pymongo.ReplaceOne({"_id": mid}, movie_complete, upsert=True))
        
        if len(batch) >= batch_size:
            db["MOVIE_COMPLETE"].bulk_write(batch)
            batch = []
            processed += batch_size
            print(f"Progression : {processed}/{total_movies} films traités...")

    if batch:
        db["MOVIE_COMPLETE"].bulk_write(batch)

    print(f"✅ Migration terminée en {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    migrate_in_batches()