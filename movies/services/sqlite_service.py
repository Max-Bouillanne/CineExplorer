import sqlite3
from django.conf import settings

class SQLiteService:
    def __init__(self):
        self.db_path = settings.DATABASES['default']['NAME']

    def _execute_query(self, query, params=()):
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_all_genres(self):
        query = "SELECT DISTINCT genre FROM GENRE ORDER BY genre ASC"
        return self._execute_query(query)


    def get_all_counts(self):
        tables = ['CHARACTER', 'DIRECTOR', 'EPISODE', 'GENRE', 'MOVIE', 'PERSON']
        counts = {}
        
        for table in tables:
            try:
                query = f"SELECT COUNT(*) as cnt FROM {table}"
                res = self._execute_query(query)
                counts[table] = res[0]['cnt'] if res else 0
            except Exception as e:
                print(f"Erreur lors du comptage de la table {table}: {e}")
                counts[table] = 0
                
        return counts

sqlite_service = SQLiteService()