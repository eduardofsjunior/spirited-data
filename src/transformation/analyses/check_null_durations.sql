SELECT
    em.film_slug,
    em.film_id,
    f.title AS ghibli_title,
    k.title AS kaggle_title,
    k.duration AS kaggle_duration
FROM {{ source('raw', 'film_emotions') }} em
LEFT JOIN {{ source('raw', 'films') }} f ON em.film_id = f.id
LEFT JOIN {{ ref('stg_kaggle_films') }} k ON em.film_id = k.film_id
WHERE k.duration IS NULL
GROUP BY em.film_slug, em.film_id, f.title, k.title, k.duration
ORDER BY em.film_slug
