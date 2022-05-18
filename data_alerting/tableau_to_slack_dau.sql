SELECT logical_or(cnt < lower_bound_scale_1_5 OR cnt > upper_bound_scale_1_5)
FROM `{{ var.value.gcp_project_name }}.tableau.alerting_dau`
WHERE date = '{{ ds }}'
