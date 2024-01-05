M = LOAD '$M' USING PigStorage(',') AS (movieID: int, year: int, title: chararray);
R = LOAD '$R' USING PigStorage(',') AS (movieID: int, userID: int, rating: int, date: chararray);

--Calculate average rating ,group by movie id
groups = GROUP R by movieID;
avg_ratings = FOREACH groups GENERATE group AS movieID,AVG(R.rating) AS avg_rating;

-- Join the datasets
joined_data = JOIN M BY movieID, avg_ratings BY movieID;

movie_avg_ratings = FOREACH joined_data GENERATE
    CONCAT((chararray)year, ': ', title  ), (double)avg_rating AS avg_rating ;


STORE movie_avg_ratings INTO '$O' USING PigStorage('\t');
