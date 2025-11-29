-- Check how emotion data film_slugs map to actual film titles
SELECT DISTINCT
    LEFT(em.film_slug, POSITION('_' IN REVERSE(em.film_slug || '_')) - 1) AS base_slug,
    em.film_id,
    f.title AS ghibli_title,
    k.title AS kaggle_title,
    k.duration
FROM {{ source('raw', 'film_emotions') }} em
LEFT JOIN {{ source('raw', 'films') }} f ON em.film_id = f.id
LEFT JOIN {{ ref('stg_kaggle_films') }} k ON em.film_id = k.film_id
ORDER BY base_slug
