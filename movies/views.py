from .models import Movie, Principal, Profession, Rating, Genre
from django.core.paginator import Paginator
from pymongo import MongoClient
from django.shortcuts import render, redirect, get_object_or_404, Http404

from django.db.models import Count
from collections import Counter

client = MongoClient("mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0")
db_mongo = client['MongoDB']

def home(request):
    if request.method == "POST":
        user_input = request.POST.get('mid', '').strip()
        
        if user_input.startswith('tt') and any(char.isdigit() for char in user_input):
            return redirect('movie_detail', mid=user_input)
        else:
            return redirect(f'/search/?q={user_input}')
    
    nb_films = Movie.objects.all().count()
    nb_acteurs = Profession.objects.filter(jobname__icontains='actor').count()
    nb_producteurs = Profession.objects.filter(jobname__icontains='producer').count()
    
    top_10 = Rating.objects.select_related('mid').filter(numvotes__gt=10000).order_by('-averagerating')[:10]
    random_movies = Movie.objects.order_by('?')[:4]

    context = {
        'total_films': nb_films,
        'total_acteurs': nb_acteurs,
        'total_producteurs': nb_producteurs,
        'top_movies': top_10,
        'random_movies': random_movies,
    }
    
    return render(request, 'movies/home.html', context)


def movie_detail(request, mid):
    movie_doc = db_mongo.MOVIE.find_one({"mid": mid})
    if not movie_doc:
        raise Http404("Film non trouvé")
        
    
    rating = db_mongo.RATING.find_one({"mid": mid})

    genres_cursor = db_mongo.GENRE.find({"mid": mid})
    movie_genres = [g['genre'] for g in genres_cursor]

    principals = list(db_mongo.PRINCIPAL.find({"mid": mid}).sort("ordering", 1))
    full_cast = []
    for p in principals:
        person = db_mongo.PERSON.find_one({"pid": p['pid']})
        char = db_mongo.CHARACTER.find_one({"mid": mid, "pid": p['pid']})
        if person:
            full_cast.append({
                'name': person.get('primaryName'),
                'category': p.get('category'),
                'character': char.get('name') if char else None,
            })

    titles = list(db_mongo.TITLE.find({"mid": mid}))
    first_genre = Genre.objects.filter(mid=mid).first()
    similars = Movie.objects.filter(genre__genre=first_genre.genre).exclude(mid=mid)[:4] if first_genre else []

    return render(request, 'movies/movie_detail.html', {
        'movie': movie_doc,
        'movie_genres': movie_genres,
        'rating': rating,
        'cast': full_cast,
        'titles': titles,
        'similars': similars
    })



def actor_films(request, nconst):
    films = Principal.objects.filter(nconst=nconst).select_related('tconst')
    return render(request, 'actor.html', {'films': films})

def benchmarks(request):
    data = {
        'labels': ['Simple Select', 'Join/Aggregate', 'Complex Filter'],
        'sqlite_times': [15, 120, 80],
        'mongo_times': [5, 40, 30]
    }
    return render(request, 'benchmarks.html', {'data': data})


def search(request):
    query = request.GET.get('q', '')
    movie_results = []
    person_results = []

    if query:
        movie_results = list(db_mongo.MOVIE.find({
            "primaryTitle": {"$regex": query, "$options": "i"}
        }).limit(10))

        person_results = list(db_mongo.PERSON.find({
            "primaryName": {"$regex": query, "$options": "i"}
        }).limit(10))

    return render(request, 'movies/search.html', {
        'query': query,
        'movies': movie_results,
        'persons': person_results
    })


def movies(request):
    genre_query = request.GET.get('genre')
    year_min = request.GET.get('year_min')
    year_max = request.GET.get('year_max')
    rating_min = request.GET.get('rating_min')
    sort_by = request.GET.get('sort', 'startyear')
    order = request.GET.get('order', 'desc')

    movies_queryset = Movie.objects.select_related('rating').all()

    sort_by = request.GET.get('sort')
    if not sort_by:
        sort_by = 'startyear'
        
    order = request.GET.get('order')
    if not order:
        order = 'desc'

    if genre_query:
        movies_queryset = movies_queryset.filter(genre__genre=genre_query)
    if year_min:
        movies_queryset = movies_queryset.filter(startyear__gte=year_min)
    if year_max:
        movies_queryset = movies_queryset.filter(startyear__lte=year_max)
    if rating_min:
        movies_queryset = movies_queryset.filter(rating__averagerating__gte=rating_min)

    sort_prefix = '-' if order == 'desc' else ''
    if sort_by == 'rating':
        movies_queryset = movies_queryset.order_by(f"{sort_prefix}rating__averagerating")
    else:
        movies_queryset = movies_queryset.order_by(f"{sort_prefix}{sort_by}")

    paginator = Paginator(movies_queryset, 20)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    all_genres = Genre.objects.values_list('genre', flat=True).distinct().order_by('genre')

    context = {
        'page_obj': page_obj,
        'all_genres': all_genres,
        'current_params': request.GET.dict(),
    }
    return render(request, 'movies/movies.html', context)



def stats(request):
    raw_genres = list(db_mongo.GENRE.aggregate([
        {"$group": {"_id": "$genre", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]))
    genres = [{'label': g['_id'], 'count': g['count']} for g in raw_genres]


    raw_decades = list(db_mongo.MOVIE.aggregate([
        {"$match": {"startYear": {"$ne": None}}},
        {"$project": {"decade": {"$subtract": ["$startYear", {"$mod": ["$startYear", 10]}]}}},
        {"$group": {"_id": "$decade", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]))
    decades = [{'label': int(d['_id']), 'count': d['count']} for d in raw_decades if d['_id']]

    raw_ratings = list(db_mongo.RATING.aggregate([
        {"$project": {"score": {"$floor": "$averageRating"}}},
        {"$group": {"_id": "$score", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]))
    ratings = [{'label': int(r['_id']), 'count': r['count']} for r in raw_ratings]

    raw_actors = list(db_mongo.PRINCIPAL.aggregate([
        {"$match": {"category": {"$in": ["actor", "actress"]}}},
        {"$group": {"_id": "$pid", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]))
    
    actors = []
    for a in raw_actors:
        p = db_mongo.PERSON.find_one({"pid": a['_id']})
        actors.append({
            'label': p.get('primaryName', 'Inconnu') if p else "Inconnu",
            'count': a['count']
        })

    return render(request, 'movies/stats.html', {
        'genres': genres,
        'decades': decades,
        'ratings': ratings,
        'actors': actors
    })