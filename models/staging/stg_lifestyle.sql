SELECT * FROM 
{{ source('GCP Dataset', 'full_data') }}