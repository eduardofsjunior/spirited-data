{% test assert_no_failed_validations(model, column_name) %}

{#
Custom dbt test to ensure no validation failures exist in emotion data quality checks.

Purpose: Quality gate for Epic 3.6 - blocks pipeline if emotion data extends beyond
film runtime + 10-minute buffer.

Usage in schema.yml:
  tests:
    - assert_no_failed_validations:
        column_name: validation_status

Test Logic:
  - Counts records where validation_status = 'FAIL'
  - Excludes summary row (film_slug = '_SUMMARY_')
  - Returns count if > 0 (test FAILS)
  - Returns empty if count = 0 (test PASSES)

Expected Result (Story 3.6.4):
  - 0 failures after emotion data regeneration
  - Test PASSES, pipeline proceeds

Actual Result (Story 3.6.5 implementation):
  - 9 failures identified (films with 13-139 minute overruns)
  - Test will FAIL, blocking pipeline
  - These are legitimate data quality issues requiring investigation

Note: UNKNOWN validation status (films without runtime data) do NOT cause test failure.
#}

SELECT
    film_slug,
    language_code,
    {{ column_name }} as validation_status,
    max_minute_offset,
    expected_duration_minutes,
    overrun_minutes
FROM {{ model }}
WHERE {{ column_name }} = 'FAIL'
  AND film_slug != '_SUMMARY_'  -- Exclude summary row

{% endtest %}
