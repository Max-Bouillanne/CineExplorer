from pymongo import MongoClient
from django.conf import settings

class MongoService:
    def __init__(self):

        self.client = MongoClient(settings.MONGO_URI)
        self.db = self.client[settings.MONGO_DB_NAME]

    def get_movie_by_id(self, tconst):
        return self.db.MOVIE.find_one({"tconst": tconst})

    def get_top_rated_movies(self, limit=10):
        return list(self.db.MOVIE.find().sort("averageRating", -1).limit(limit))

    def get_movies_by_genre(self, genre):
        return list(self.db.MOVIE.find({"genres": {"$regex": genre, "$options": "i"}}).limit(20))

mongo_service = MongoService()