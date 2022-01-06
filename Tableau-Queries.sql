/*
Streaming Service Data Exploration 2021: Tableau Visualizations
Kailas Pillai
*/

-- Table 1: Number of Movies and TV Shows offered by each service
        
    SELECT streaming_service, COUNT(movie_runtime) AS number_of_movies, COUNT(number_of_seasons) as number_of_tv_shows
    FROM `test-327523.streaming_services_project.combined_titles`
    GROUP BY streaming_service;


-- Table 2: Number of titles produced per country

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
    )

    SELECT o_count.x as country, o_count.cnt as number_of_overall_titles
    FROM o_count
    ORDER BY o_count.cnt DESC;

-- Table 3: Number of titles offered by each service ordered by release year

    Select release_year, COUNT(release_year) as number_of_titles 
    FROM `test-327523.streaming_services_project.amazon_prime_titles`
    Group by release_year
    Order by release_year;

    Select release_year, COUNT(release_year) as number_of_titles 
    FROM `test-327523.streaming_services_project.disney_plus_titles`
    Group by release_year
    Order by release_year;

    Select release_year, COUNT(release_year) as number_of_hulu_titles 
    FROM `test-327523.streaming_services_project.hulu_titles`
    Group by release_year
    Order by release_year;

    Select release_year, COUNT(release_year) as number_of_netflix_titles 
    FROM `test-327523.streaming_services_project.netflix_titles`
    Group by release_year
    Order by release_year;


-- Table 4: Most popular genres per service

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


-- Table 5: Number of titles added per year by each service

    -- no data for Amazon as mentioned in the Streaming Data Exploration

    Select EXTRACT(year from date_added) as year, COUNT(date_added) as number_of_titles 
    FROM `test-327523.streaming_services_project.disney_plus_titles`
    Group by year
    Order by year;

    Select EXTRACT(year from date_added) as year, COUNT(date_added) as number_of_titles 
    FROM `test-327523.streaming_services_project.hulu_titles`
    Group by year
    Order by year;

    Select EXTRACT(year from date_added) as year, COUNT(date_added) as number_of_titles 
    FROM `test-327523.streaming_services_project.netflix_titles`
    Group by year
    Order by year;

    -- New columns were added in Google Sheets with the accumulated totals across years
