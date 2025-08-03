SELECT assert_true(
    (
    SELECT COUNT(*) 
    FROM dev.metadata.data_quality_checks
    WHERE date = current_date()) <= 1,
    'DQ failures. Check the display_failures task for more information.'
);