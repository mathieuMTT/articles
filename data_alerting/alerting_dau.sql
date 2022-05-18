######################################################################################################
# GET DAU LAST 90 DAYS
######################################################################################################
       WITH dau_cnt AS (
     SELECT date,
            dim,
            dim_value,
            cnt
       FROM (
     SELECT _PARTITIONDATE AS date,
            DATE_SUB(_PARTITIONDATE, INTERVAL 90 DAY) AS start_date,
            'total' AS dim,
            'total' AS dim_value,
            SUM(dau_cnt) AS cnt
       FROM `happn.organizations`
      WHERE _PARTITIONDATE >= DATE_SUB(current_date, INTERVAL 180 DAY)
   GROUP BY 1, 2, 3, 4
  UNION ALL
     SELECT _PARTITIONDATE AS date,
            DATE_SUB(_PARTITIONDATE, INTERVAL 90 DAY) AS start_date,
            'last_os' AS dim,
            last_os AS dim_value,
            SUM(dau_cnt) AS cnt
       FROM `happn.organizations`
      WHERE _PARTITIONDATE >= DATE_SUB(current_date, INTERVAL 180 DAY)
        AND last_os IN ("android", "ios")
   GROUP BY 1, 2, 3, 4
  UNION ALL
     SELECT _PARTITIONDATE AS date,
            DATE_SUB(_PARTITIONDATE, INTERVAL 90 DAY) AS start_date,
           'gender_d0' AS dim,
           gender_d0 AS dim_value,
           SUM(dau_cnt) AS cnt
      FROM `happn.organizations`
     WHERE _PARTITIONDATE >= DATE_SUB(current_date, INTERVAL 180 DAY)
       AND gender_d0 IN ("male", "female")
  GROUP BY 1, 2, 3, 4
 UNION ALL
    SELECT _PARTITIONDATE AS date,
           DATE_SUB(_PARTITIONDATE, INTERVAL 90 DAY) AS start_date,
           'reg_vintage' AS dim,
           CASE WHEN reg_vintage_group = "NEW" THEN "NEW"
                WHEN reg_vintage_group = "1-7 D" THEN "1-7 D"
                ELSE "old reg"
           END AS dim_value,
           SUM(dau_cnt) AS cnt
      FROM `happn.organizations`
     WHERE _PARTITIONDATE >= DATE_SUB(current_date, INTERVAL 180 DAY)
  GROUP BY 1, 2, 3, 4)),

######################################################################################################
# GET DAU QUANTILES
######################################################################################################
            quantiles_cnt AS (
     SELECT dau_cnt.date AS date,
            dau_cnt.dim,
            dau_cnt.dim_value,
            dau_cnt.cnt AS cnt,
            APPROX_QUANTILES(histo.cnt, 4)[OFFSET(1)] AS quantile_25,
            APPROX_QUANTILES(histo.cnt, 4)[OFFSET(3)] AS quantile_75
       FROM dau_cnt
 CROSS JOIN dau_cnt histo
      WHERE dau_cnt.date >= DATE_SUB(current_date, INTERVAL 90 DAY)
        AND dau_cnt.date > histo.date
        AND DATE_DIFF(dau_cnt.date, histo.date, day) <= 90
        AND dau_cnt.dim = histo.dim
        AND dau_cnt.dim_value = histo.dim_value
   GROUP BY 1, 2, 3, 4)
            
######################################################################################################
# GET FINAL RESULTS
######################################################################################################
     SELECT date,
            dim,
            dim_value,
            cnt,
            quantile_25,
            quantile_75,
            quantile_25 - 1.5 * (quantile_75 - quantile_25) AS lower_bound_scale_1_5,
            quantile_75 + 1.5 * (quantile_75 - quantile_25) AS upper_bound_scale_1_5,
            quantile_25 - 1 * (quantile_75 - quantile_25) AS lower_bound_scale_1,
            quantile_75 + 1 * (quantile_75 - quantile_25) AS upper_bound_scale_1,
            quantile_25 - 2 * (quantile_75 - quantile_25) AS lower_bound_scale_2,
            quantile_75 + 2 * (quantile_75 - quantile_25) AS upper_bound_scale_2,
            quantile_25 - 2.5 * (quantile_75 - quantile_25) AS lower_bound_scale_2_5,
            quantile_75 + 2.5 * (quantile_75 - quantile_25) AS upper_bound_scale_2_5
       FROM quantiles_cnt
