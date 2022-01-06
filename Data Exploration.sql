/*
Streaming Service Data Exploration 2021 
Kailas Pillai
*/

-- [Part I of II] The primary part of the data exploration will focus on all titles offered by Netflix, Hulu, Amazon Prime, and Disney Plus
-- We will be using four datasets for this portion: 'netflix_titles', 'hulu_titles', 'amazon_prime_titles', and 'disney_plus_titles'

-- Query netflix_titles, hulu_titles, amazon_prime_titles, and disney_plus_titles before combining

Select *
From `test-327523.streaming_services_project.netflix_titles`
order by 1;

Select *
From `test-327523.streaming_services_project.hulu_titles`
order by 1;

Select *
From `test-327523.streaming_services_project.amazon_prime_titles`
order by 1;

Select *
From `test-327523.streaming_services_project.disney_plus_titles`
order by 1;

-- Append netflix_titles, hulu_titles, amazon_prime_titles, and disney_plus_titles, ordered by streaming_service and respective show_id

SELECT *
FROM `test-327523.streaming_services_project.netflix_titles`
UNION ALL 
SELECT *
FROM `test-327523.streaming_services_project.hulu_titles`
UNION ALL 
SELECT *
FROM `test-327523.streaming_services_project.amazon_prime_titles`
UNION ALL 
SELECT *
FROM `test-327523.streaming_services_project.disney_plus_titles`
order by 2,1;

-- Using CTE to calculate total number of titles offered by Netflix, Hulu, Amazon Prime, and Disney Plus that were released in 2021

WITH combined_streaming_data AS (
SELECT *
FROM `test-327523.streaming_services_project.netflix_titles`
UNION ALL 
SELECT *
FROM `test-327523.streaming_services_project.hulu_titles`
UNION ALL 
SELECT *
FROM `test-327523.streaming_services_project.amazon_prime_titles`
UNION ALL 
SELECT *
FROM `test-327523.streaming_services_project.disney_plus_titles`
order by 2,1
)

SELECT COUNT (*) 
FROM combined_streaming_data 
WHERE release_year = 2021;

-- Since BigQuery does not support true Temp Tables and only allows CTE, will be creating regular tables instead
-- Creating new table that contains all the combined streaming data information for Netflix, Hulu, Amazon Prime, and Disney Plus for further exploration

DROP TABLE IF EXISTS streaming_services_project.combined_titles;
CREATE TABLE streaming_services_project.combined_titles AS (
    SELECT *
    FROM `test-327523.streaming_services_project.netflix_titles`
    UNION ALL 
    SELECT *
    FROM `test-327523.streaming_services_project.hulu_titles`
    UNION ALL 
    SELECT *
    FROM `test-327523.streaming_services_project.amazon_prime_titles`
    UNION ALL 
    SELECT *
    FROM `test-327523.streaming_services_project.disney_plus_titles`
    order by 2,1
);

-- Using new table to find out how many of the 2021 releases were produced (at least partly) in the United States

SELECT COUNT (*) 
FROM `test-327523.streaming_services_project.combined_titles`
WHERE release_year = 2021 AND 'United States' IN(country1,country2,country3,country4,country5,country6);


-- Splitting the new table 'combined_streaming_data' into two more new tables containing just movies and just TV shows

    -- Creating the new tables for movies across all services

    DROP TABLE IF EXISTS streaming_services_project.combined_movies;
    CREATE TABLE streaming_services_project.combined_movies AS (
        SELECT *
        FROM `test-327523.streaming_services_project.netflix_titles`
        WHERE type = 'Movie'
        UNION ALL 
        SELECT *
        FROM `test-327523.streaming_services_project.hulu_titles`
        WHERE type = 'Movie'
        UNION ALL 
        SELECT *
        FROM `test-327523.streaming_services_project.amazon_prime_titles`
        WHERE type = 'Movie'
        UNION ALL 
        SELECT *
        FROM `test-327523.streaming_services_project.disney_plus_titles`
        WHERE type = 'Movie'
        order by 2,1
    );

    -- Creating the new tables for TV shows across all services

    DROP TABLE IF EXISTS streaming_services_project.combined_tv_shows;
    CREATE TABLE streaming_services_project.combined_tv_shows AS (
        SELECT *
        FROM `test-327523.streaming_services_project.netflix_titles`
        WHERE type = 'TV Show'
        UNION ALL 
        SELECT *
        FROM `test-327523.streaming_services_project.hulu_titles`
        WHERE type = 'TV Show'
        UNION ALL 
        SELECT *
        FROM `test-327523.streaming_services_project.amazon_prime_titles`
        WHERE type = 'TV Show'
        UNION ALL 
        SELECT *
        FROM `test-327523.streaming_services_project.disney_plus_titles`
        WHERE type = 'TV Show'
        order by 2,1
    );

-- Movies vs. TV Shows
-- Using CTE to calculate number of each title type, total number of titles offered by service, and percentage of each type offered by each service

-- 1. Netflix

    -- Netflix Movie Count, Total, and Percentage
    WITH n_mov AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.netflix_titles`
    WHERE type = 'Movie'
    ), n_tot AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.netflix_titles`
    )

    SELECT n_mov.cnt as number_of_movies, n_tot.cnt as total_netflix_titles, (CAST(n_mov.cnt AS FLOAT64) / n_tot.cnt)*100 as percentage_movies
    FROM n_mov, n_tot;

    -- Netflix TV Show Count, Total, and Percentage
    WITH n_tv AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.netflix_titles`
    WHERE type = 'TV Show'
    ), n_tot AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.netflix_titles`
    )

    SELECT n_tv.cnt as number_of_tv_shows, n_tot.cnt as total_netflix_titles, (CAST(n_tv.cnt AS FLOAT64) / n_tot.cnt)*100 as percentage_tv_shows
    FROM n_tv, n_tot;

-- 2. Hulu

    -- Hulu Movie Count, Total, and Percentage
    WITH h_mov AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.hulu_titles`
    WHERE type = 'Movie'
    ), h_tot AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.hulu_titles`
    )

    SELECT h_mov.cnt as number_of_movies, h_tot.cnt as total_hulu_titles, (CAST(h_mov.cnt AS FLOAT64) / h_tot.cnt)*100 as percentage_movies
    FROM h_mov, h_tot;

    -- Hulu TV Show Count, Total, and Percentage
    WITH h_tv AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.hulu_titles`
    WHERE type = 'TV Show'
    ), h_tot AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.hulu_titles`
    )

    SELECT h_tv.cnt as number_of_tv_shows, h_tot.cnt as total_hulu_titles, (CAST(h_tv.cnt AS FLOAT64) / h_tot.cnt)*100 as percentage_tv_shows
    FROM h_tv, h_tot;

-- 3. Amazon Prime

    -- Amazon Prime Movie Count, Total, and Percentage
    WITH a_mov AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.amazon_prime_titles`
    WHERE type = 'Movie'
    ), a_tot AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.amazon_prime_titles`
    )

    SELECT a_mov.cnt as number_of_movies, a_tot.cnt as total_amazon_titles, (CAST(a_mov.cnt AS FLOAT64) / a_tot.cnt)*100 as percentage_movies
    FROM a_mov, a_tot;

    -- Amazon Prime TV Show Count, Total, and Percentage
    WITH a_tv AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.amazon_prime_titles`
    WHERE type = 'TV Show'
    ), a_tot AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.amazon_prime_titles`
    )

    SELECT a_tv.cnt as number_of_tv_shows, a_tot.cnt as total_amazon_titles, (CAST(a_tv.cnt AS FLOAT64) / a_tot.cnt)*100 as percentage_tv_shows
    FROM a_tv, a_tot;

-- 4. Disney Plus

    -- Disney Plus Movie Count, Total, and Percentage
    WITH d_mov AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.disney_plus_titles`
    WHERE type = 'Movie'
    ), d_tot AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.disney_plus_titles`
    )

    SELECT d_mov.cnt as number_of_movies, d_tot.cnt as total_disney_titles, (CAST(d_mov.cnt AS FLOAT64) / d_tot.cnt)*100 as percentage_movies
    FROM d_mov, d_tot;

    -- Disney Plus TV Show Count, Total, and Percentage
    WITH d_tv AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.disney_plus_titles`
    WHERE type = 'TV Show'
    ), d_tot AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.disney_plus_titles`
    )

    SELECT d_tv.cnt as number_of_tv_shows, d_tot.cnt as total_disney_titles, (CAST(d_tv.cnt AS FLOAT64) / d_tot.cnt)*100 as percentage_tv_shows
    FROM d_tv, d_tot;

-- 5. Overall

    -- Overall Movie Count, Total, and Percentage
    WITH o_mov AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.combined_movies`
    ), o_tot AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.combined_titles`
    )

    SELECT o_mov.cnt as number_of_movies, o_tot.cnt as total_overall_titles, (CAST(o_mov.cnt AS FLOAT64) / o_tot.cnt)*100 as percentage_movies
    FROM o_mov, o_tot;

    -- Overall TV Show Count, Total, and Percentage
    WITH o_tv AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.combined_tv_shows`
    ), o_tot AS (
    SELECT COUNT(*) AS cnt
    FROM `test-327523.streaming_services_project.combined_titles`
    )

    SELECT o_tv.cnt as number_of_tv_shows, o_tot.cnt as total_overall_titles, (CAST(o_tv.cnt AS FLOAT64) / o_tot.cnt)*100 as percentage_tv_shows
    FROM o_tv, o_tot;


-- Top 10 Most Frequent Countries of Production, Number of Titles, and Percentage of Overall
-- Scans six columns of each table to find the most common countries of production for each service, calculates percentage from each of the top 10 countries

-- 1. Netflix

    WITH n_count AS (
        SELECT country as x, COUNT(*) as cnt
        FROM (
            SELECT country1 AS country
            FROM `test-327523.streaming_services_project.netflix_titles`
            UNION ALL
            SELECT country2 AS country
            FROM `test-327523.streaming_services_project.netflix_titles`
            UNION ALL
            SELECT country3 AS country
            FROM `test-327523.streaming_services_project.netflix_titles`
            UNION ALL
            SELECT country4 AS country
            FROM `test-327523.streaming_services_project.netflix_titles`
            UNION ALL
            SELECT country5 AS country
            FROM `test-327523.streaming_services_project.netflix_titles`
            UNION ALL
            SELECT country6 AS country
            FROM `test-327523.streaming_services_project.netflix_titles`
            )
        where country is not null
        GROUP BY country
        ORDER BY cnt DESC
    ), n_total AS (
        SELECT COUNT(*) as cnt
        FROM `test-327523.streaming_services_project.netflix_titles`
    )

    SELECT n_count.x as country, n_count.cnt as number_of_netflix_titles, (CAST(n_count.cnt AS FLOAT64) / n_total.cnt)*100 as percentage_from_country
    FROM n_count, n_total
    ORDER BY n_count.cnt DESC
    LIMIT 10;


-- 2. Hulu

    WITH h_count AS (
        SELECT country as x, COUNT(*) as cnt
        FROM (
            SELECT country1 AS country
            FROM `test-327523.streaming_services_project.hulu_titles`
            UNION ALL
            SELECT country2 AS country
            FROM `test-327523.streaming_services_project.hulu_titles`
            UNION ALL
            SELECT country3 AS country
            FROM `test-327523.streaming_services_project.hulu_titles`
            UNION ALL
            SELECT country4 AS country
            FROM `test-327523.streaming_services_project.hulu_titles`
            UNION ALL
            SELECT country5 AS country
            FROM `test-327523.streaming_services_project.hulu_titles`
            UNION ALL
            SELECT country6 AS country
            FROM `test-327523.streaming_services_project.hulu_titles`
            )
        where country is not null
        GROUP BY country
        ORDER BY cnt DESC
    ), h_total AS (
        SELECT COUNT(*) as cnt
        FROM `test-327523.streaming_services_project.hulu_titles`
    )

    SELECT h_count.x as country, h_count.cnt as number_of_hulu_titles, (CAST(h_count.cnt AS FLOAT64) / h_total.cnt)*100 as percentage_from_country
    FROM h_count, h_total
    ORDER BY h_count.cnt DESC
    LIMIT 10;

-- 3. Amazon Prime: does not contain enough country information for results to be reliable

-- 4. Disney Plus

    WITH d_count AS (
        SELECT country as x, COUNT(*) as cnt
        FROM (
            SELECT country1 AS country
            FROM `test-327523.streaming_services_project.disney_plus_titles`
            UNION ALL
            SELECT country2 AS country
            FROM `test-327523.streaming_services_project.disney_plus_titles`
            UNION ALL
            SELECT country3 AS country
            FROM `test-327523.streaming_services_project.disney_plus_titles`
            UNION ALL
            SELECT country4 AS country
            FROM `test-327523.streaming_services_project.disney_plus_titles`
            UNION ALL
            SELECT country5 AS country
            FROM `test-327523.streaming_services_project.disney_plus_titles`
            UNION ALL
            SELECT country6 AS country
            FROM `test-327523.streaming_services_project.disney_plus_titles`
            )
        where country is not null
        GROUP BY country
        ORDER BY cnt DESC
    ), d_total AS (
        SELECT COUNT(*) as cnt
        FROM `test-327523.streaming_services_project.disney_plus_titles`
    )

    SELECT d_count.x as country, d_count.cnt as number_of_disney_titles, (CAST(d_count.cnt AS FLOAT64) / d_total.cnt)*100 as percentage_from_country
    FROM d_count, d_total
    ORDER BY d_count.cnt DESC
    LIMIT 10;

-- 5. Overall

    WITH o_count AS (
        SELECT country as x, COUNT(*) as cnt
        FROM (
            SELECT country1 AS country
            FROM `test-327523.streaming_services_project.combined_titles`
            UNION ALL
            SELECT country2 AS country
            FROM `test-327523.streaming_services_project.combined_titles`
            UNION ALL
            SELECT country3 AS country
            FROM `test-327523.streaming_services_project.combined_titles`
            UNION ALL
            SELECT country4 AS country
            FROM `test-327523.streaming_services_project.combined_titles`
            UNION ALL
            SELECT country5 AS country
            FROM `test-327523.streaming_services_project.combined_titles`
            UNION ALL
            SELECT country6 AS country
            FROM `test-327523.streaming_services_project.combined_titles`
            )
        where country is not null
        GROUP BY country
        ORDER BY cnt DESC
    ), o_total AS (
        SELECT COUNT(*) as cnt
        FROM `test-327523.streaming_services_project.combined_titles`
    )

    SELECT o_count.x as country, o_count.cnt as number_of_overall_titles, (CAST(o_count.cnt AS FLOAT64) / o_total.cnt)*100 as percentage_from_country
    FROM o_count, o_total
    ORDER BY o_count.cnt DESC
    LIMIT 10;


-- Average number and maximum of seasons among each service's title 

-- 1. Netflix

SELECT AVG(number_of_seasons) as average_number_of_seasons, max(number_of_seasons) as max_number_of_seasons
FROM `test-327523.streaming_services_project.netflix_titles`
WHERE type = 'TV Show';

-- 2. Hulu

SELECT AVG(number_of_seasons) as average_number_of_seasons, max(number_of_seasons) as max_number_of_seasons
FROM `test-327523.streaming_services_project.hulu_titles`
WHERE type = 'TV Show';

-- 3. Amazon Prime

SELECT AVG(number_of_seasons) as average_number_of_seasons, max(number_of_seasons) as max_number_of_seasons
FROM `test-327523.streaming_services_project.amazon_prime_titles`
WHERE type = 'TV Show';

-- 4. Disney Plus

SELECT AVG(number_of_seasons) as average_number_of_seasons, max(number_of_seasons) as max_number_of_seasons
FROM `test-327523.streaming_services_project.disney_plus_titles`
WHERE type = 'TV Show';

-- 5. Overall

SELECT AVG(number_of_seasons) as average_number_of_seasons, max(number_of_seasons) as max_number_of_seasons
FROM `test-327523.streaming_services_project.combined_tv_shows`;


-- Average and maximum movie runtime in minutes 

-- 1. Netflix

SELECT AVG(movie_runtime) AS average_movie_runtime, max(movie_runtime) AS max_movie_runtime
FROM `test-327523.streaming_services_project.netflix_titles`
WHERE type = 'Movie';

-- 2. Hulu

SELECT AVG(movie_runtime) AS average_movie_runtime, max(movie_runtime) AS max_movie_runtime
FROM `test-327523.streaming_services_project.hulu_titles`
WHERE type = 'Movie';

-- 3. Amazon Prime

SELECT AVG(movie_runtime) AS average_movie_runtime, max(movie_runtime) AS max_movie_runtime
FROM `test-327523.streaming_services_project.amazon_prime_titles`
WHERE type = 'Movie';

-- 4. Disney Plus

SELECT AVG(movie_runtime) AS average_movie_runtime, max(movie_runtime) AS max_movie_runtime
FROM `test-327523.streaming_services_project.disney_plus_titles`
WHERE type = 'Movie';

-- 5. Overall

SELECT AVG(movie_runtime) AS average_movie_runtime, max(movie_runtime) AS max_movie_runtime
FROM `test-327523.streaming_services_project.combined_movies`;



-- Most popular genres per service

-- Netflix

    WITH n_genre AS (
        SELECT genre as x, COUNT(*) as cnt
        FROM (
            SELECT category1 AS genre
            FROM `test-327523.streaming_services_project.netflix_titles`
            UNION ALL
            SELECT category2 AS genre
            FROM `test-327523.streaming_services_project.netflix_titles`
            UNION ALL
            SELECT category3 AS genre
            FROM `test-327523.streaming_services_project.netflix_titles`
            UNION ALL
            SELECT category4 AS genre
            FROM `test-327523.streaming_services_project.netflix_titles`
            UNION ALL
            SELECT category5 AS genre
            FROM `test-327523.streaming_services_project.netflix_titles`
            )
        where genre is not null
        GROUP BY genre
        ORDER BY cnt DESC
    )

    SELECT n_genre.x as genre, n_genre.cnt as titles
    FROM n_genre
    ORDER BY n_genre.cnt DESC
    LIMIT 25;

-- Hulu

    WITH h_genre AS (
        SELECT genre as x, COUNT(*) as cnt
        FROM (
            SELECT category1 AS genre
            FROM `test-327523.streaming_services_project.hulu_titles`
            UNION ALL
            SELECT category2 AS genre
            FROM `test-327523.streaming_services_project.hulu_titles`
            UNION ALL
            SELECT category3 AS genre
            FROM `test-327523.streaming_services_project.hulu_titles`
            UNION ALL
            SELECT category4 AS genre
            FROM `test-327523.streaming_services_project.hulu_titles`
            UNION ALL
            SELECT category5 AS genre
            FROM `test-327523.streaming_services_project.hulu_titles`
            )
        where genre is not null
        GROUP BY genre
        ORDER BY cnt DESC
    )

    SELECT h_genre.x as genre, h_genre.cnt as titles
    FROM h_genre
    ORDER BY h_genre.cnt DESC
    LIMIT 25;

-- Amazon Prime

    WITH a_genre AS (
        SELECT genre as x, COUNT(*) as cnt
        FROM (
            SELECT category1 AS genre
            FROM `test-327523.streaming_services_project.amazon_prime_titles`
            UNION ALL
            SELECT category2 AS genre
            FROM `test-327523.streaming_services_project.amazon_prime_titles`
            UNION ALL
            SELECT category3 AS genre
            FROM `test-327523.streaming_services_project.amazon_prime_titles`
            UNION ALL
            SELECT category4 AS genre
            FROM `test-327523.streaming_services_project.amazon_prime_titles`
            UNION ALL
            SELECT category5 AS genre
            FROM `test-327523.streaming_services_project.amazon_prime_titles`
            )
        where genre is not null
        GROUP BY genre
        ORDER BY cnt DESC
    )

    SELECT a_genre.x as genre, a_genre.cnt as titles
    FROM a_genre
    ORDER BY a_genre.cnt DESC
    LIMIT 25;

-- Disney Plus

    WITH d_genre AS (
        SELECT genre as x, COUNT(*) as cnt
        FROM (
            SELECT category1 AS genre
            FROM `test-327523.streaming_services_project.disney_plus_titles`
            UNION ALL
            SELECT category2 AS genre
            FROM `test-327523.streaming_services_project.disney_plus_titles`
            UNION ALL
            SELECT category3 AS genre
            FROM `test-327523.streaming_services_project.disney_plus_titles`
            UNION ALL
            SELECT category4 AS genre
            FROM `test-327523.streaming_services_project.disney_plus_titles`
            UNION ALL
            SELECT category5 AS genre
            FROM `test-327523.streaming_services_project.disney_plus_titles`
            )
        where genre is not null
        GROUP BY genre
        ORDER BY cnt DESC
    )

    SELECT d_genre.x as genre, d_genre.cnt as titles
    FROM d_genre
    ORDER BY d_genre.cnt DESC
    LIMIT 25;



-- This next part of the data exploration will focus on exploring Netflix Originals in comparison to other titles offered by Netflix
-- Two new datasets will be used here: 'netflix_expanded_info' containing all Netflix titles with expanded information on each and 'netflix_originals' containing only Netflix Originals


-- Query netflix_expanded_info and netflix_originals

Select *
From `test-327523.streaming_services_project.netflix_expanded_info`
ORDER by IF((Release_Date is not null), Release_Date, Netflix_Release_Date) DESC; 

Select *
From `test-327523.streaming_services_project.netflix_originals`
Order by Premiere DESC;


-- Change row names and sorting orders of the tables to prepare for JOIN

SELECT
    * -- EXCEPT(Title,Genre,Runtime,IMDb_Score,Language,Language1,Language2,Language3),
    -- Title AS ogtitle, Genre as oggenre, Runtime as ogruntime, IMDb_Score as ogimdbscore, Language as oglanguage,
    -- Language1 as oglanguage1, Language2 as oglanguage2, Language3 as oglanguage3
FROM
    `test-327523.streaming_services_project.netflix_originals`
    Order by Premiere DESC;

SELECT *
FROM `test-327523.streaming_services_project.netflix_expanded_info`
ORDER by IF((Release_Date is not null), Release_Date, Netflix_Release_Date) DESC;


-- Use JOIN in order to combine tables on basis of Title and primary language (just Title, if language doesn't match), 
-- CASE to indicate matches, and create a new table out of the result called combined_netflix

CREATE TABLE IF NOT EXISTS streaming_services_project.combined_netflix AS(
SELECT *,
CASE WHEN `test-327523.streaming_services_project.netflix_originals`.ogtitle IS NULL THEN 0 ELSE 1 END as Match
FROM `test-327523.streaming_services_project.netflix_expanded_info` LEFT OUTER JOIN `test-327523.streaming_services_project.netflix_originals`
ON IF((`test-327523.streaming_services_project.netflix_expanded_info`.language1 = `test-327523.streaming_services_project.netflix_originals`.oglanguage1), 
(`test-327523.streaming_services_project.netflix_expanded_info`.Title = `test-327523.streaming_services_project.netflix_originals`.ogtitle
AND `test-327523.streaming_services_project.netflix_expanded_info`.language1 = `test-327523.streaming_services_project.netflix_originals`.oglanguage1),
(`test-327523.streaming_services_project.netflix_expanded_info`.Title = `test-327523.streaming_services_project.netflix_originals`.ogtitle))
ORDER BY IF((Release_Date is not null), Release_Date, IF((Netflix_Release_Date is not null), Netflix_Release_Date, Premiere)) DESC
);


-- Making sure to see how many matches there were between the Netflix Originals list and the overall database

SELECT SUM(Match) as Matches
FROM `test-327523.streaming_services_project.combined_netflix`;

-- Comparing number of Netflix Original and outside content titles, as well as respective average IMDb scores

WITH n_og AS (
SELECT AVG(IMDb_Score) AS avrg, COUNT(Title) as cnt
FROM `test-327523.streaming_services_project.combined_netflix`
WHERE ogtitle is not null
), n_rest AS (
SELECT AVG(IMDb_Score) AS avrg, COUNT (Title) as cnt
FROM `test-327523.streaming_services_project.combined_netflix`
WHERE ogtitle is null
)

SELECT n_og.cnt as netflix_original_Number_of_titles, n_og.avrg as netflix_original_Average_IMDb_score, 
n_rest.cnt as netflix_outside_content_Number_of_titles, n_rest.avrg as netflix_outside_content_Average_IMDb_score
FROM n_og, n_rest;

-- Comparing number of Netflix Original and outside content titles only released on Netflix in 2020 and 2021 (first 3 months of 2021), as well as respective average IMDb scores

WITH n_og AS (
SELECT AVG(IMDb_Score) AS avrg, COUNT(Title) as cnt
FROM `test-327523.streaming_services_project.combined_netflix`
WHERE ogtitle is not null and EXTRACT(year from Netflix_Release_Date) IN (2020, 2021)
), n_rest AS (
SELECT AVG(IMDb_Score) AS avrg, COUNT (Title) as cnt
FROM `test-327523.streaming_services_project.combined_netflix`
WHERE ogtitle is null and EXTRACT(year from Netflix_Release_Date) IN (2020, 2021) 
)

SELECT n_og.cnt as netflix_original_Number_of_titles_2021, n_og.avrg as netflix_original_Average_IMDb_score_2021, 
n_rest.cnt as netflix_outside_content_Number_of_titles_2021, n_rest.avrg as netflix_outside_content_Average_IMDb_score_2021
FROM n_og, n_rest;

-- Overall number of titles released on Netflix every year

SELECT EXTRACT(year from Netflix_Release_Date) as year, COUNT(*) as total_number_of_titles, 
FROM `test-327523.streaming_services_project.combined_netflix`
where Release_Date is not null
GROUP BY year
ORDER BY year;

-- Showing year-by-year number of original releases and average IMDb Score of Netflix originals (when titles were added to Netflix, not original release)

WITH n_og AS (
SELECT AVG(IMDb_Score) AS avrg, COUNT(Title) as cnt, EXTRACT(year from Netflix_Release_Date) as year
FROM `test-327523.streaming_services_project.combined_netflix`
WHERE ogtitle is not null
GROUP BY year
), n_rest AS (
SELECT AVG(IMDb_Score) AS avrg, COUNT (Title) as cnt, EXTRACT(year from Netflix_Release_Date) as year
FROM `test-327523.streaming_services_project.combined_netflix`
WHERE ogtitle is null
GROUP BY year
)

SELECT n_og.year as year, n_og.cnt as netflix_original_Number_of_titles, n_og.avrg as netflix_original_Average_IMDb_score
FROM n_og
ORDER BY year;

-- Showing year-by-year number of original releases and average IMDb Score of Netflix outside content (when titles were added to Netflix, not original release)

WITH n_og AS (
SELECT AVG(IMDb_Score) AS avrg, COUNT(Title) as cnt, EXTRACT(year from Netflix_Release_Date) as year
FROM `test-327523.streaming_services_project.combined_netflix`
WHERE ogtitle is not null
GROUP BY year
), n_rest AS (
SELECT AVG(IMDb_Score) AS avrg, COUNT (Title) as cnt, EXTRACT(year from Netflix_Release_Date) as year
FROM `test-327523.streaming_services_project.combined_netflix`
WHERE ogtitle is null
GROUP BY year
)

SELECT n_rest.year as year, n_rest.cnt as netflix_outside_content_Number_of_titles, n_rest.avrg as netflix_outside_content_Average_IMDb_score
FROM n_rest
ORDER BY year;
