SELECT SUM(CAST(t1.value_result AS DOUBLE PRECISION)) AS sum
FROM get_pset_value('ifcroof', 'Dimensions', 'Area', '1=1') AS t1;