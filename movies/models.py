from django.db import models

class Movie(models.Model):
    mid = models.CharField(primary_key=True, max_length=20) 
    titletype = models.CharField(db_column='titleType', max_length=50, blank=True, null=True)
    primarytitle = models.CharField(db_column='primaryTitle', max_length=500, blank=True, null=True)
    originaltitle = models.CharField(db_column='originalTitle', max_length=500, blank=True, null=True)
    isadult = models.IntegerField(db_column='isAdult', blank=True, null=True)
    startyear = models.IntegerField(db_column='startYear', blank=True, null=True)
    endyear = models.TextField(db_column='endYear', blank=True, null=True)
    runtimeminutes = models.TextField(db_column='runtimeMinutes', blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'MOVIE'

class Person(models.Model):
    pid = models.CharField(primary_key=True, max_length=20) 
    primaryname = models.CharField(db_column='primaryName', max_length=255, blank=True, null=True)
    birthyear = models.TextField(db_column='birthYear', blank=True, null=True)
    deathyear = models.TextField(db_column='deathYear', blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'PERSON'

class Principal(models.Model):
    mid = models.ForeignKey(Movie, models.DO_NOTHING, db_column='mid')
    ordering = models.IntegerField(blank=True, null=True)
    pid = models.ForeignKey(Person, models.DO_NOTHING, db_column='pid')
    category = models.CharField(max_length=100, blank=True, null=True)
    job = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'PRINCIPAL'


class Character(models.Model):
    mid = models.ForeignKey('Movie', on_delete=models.CASCADE, db_column='mid')
    pid = models.ForeignKey('Person', on_delete=models.CASCADE, db_column='pid')
    name = models.CharField(max_length=500, primary_key=True)

    class Meta:
        managed = False
        db_table = 'CHARACTER'
        unique_together = (('mid', 'pid', 'name'),)



class Rating(models.Model):
    mid = models.OneToOneField('Movie', on_delete=models.CASCADE, db_column='mid', primary_key=True)
    averagerating = models.FloatField(db_column='averageRating')
    numvotes = models.IntegerField(db_column='numVotes')

    class Meta:
        managed = False
        db_table = 'RATING'

class Genre(models.Model):
    mid = models.ForeignKey('Movie', on_delete=models.CASCADE, db_column='mid', primary_key=True)
    genre = models.CharField(max_length=50)

    class Meta:
        managed = False
        db_table = 'GENRE'
        unique_together = (('mid', 'genre'),)

class Profession(models.Model):
    pid = models.CharField(max_length=20, primary_key=True) 
    jobname = models.CharField(db_column='jobName', max_length=100)

    class Meta:
        managed = False 
        db_table = 'PROFESSION' 
        unique_together = (('pid', 'jobname'),) 

    def __str__(self):
        return f"{self.pid} - {self.jobname}"


class Director(models.Model):
    mid = models.ForeignKey('Movie', on_delete=models.CASCADE, db_column='mid', primary_key=True)
    pid = models.ForeignKey('Person', on_delete=models.CASCADE, db_column='pid')

    class Meta:
        managed = False
        db_table = 'DIRECTOR'
        unique_together = (('mid', 'pid'),)

class Writer(models.Model):
    mid = models.ForeignKey('Movie', on_delete=models.CASCADE, db_column='mid', primary_key=True)
    pid = models.ForeignKey('Person', on_delete=models.CASCADE, db_column='pid')

    class Meta:
        managed = False
        db_table = 'WRITER'
        unique_together = (('mid', 'pid'),)

class Title(models.Model):
    mid = models.ForeignKey('Movie', on_delete=models.CASCADE, db_column='mid')
    ordering = models.IntegerField(primary_key=True)
    title = models.CharField(max_length=500)
    region = models.CharField(max_length=10, null=True)
    language = models.CharField(max_length=10, null=True)
    isoriginaltitle = models.IntegerField(db_column='isOriginalTitle', null=True)

    class Meta:
        managed = False
        db_table = 'TITLE'