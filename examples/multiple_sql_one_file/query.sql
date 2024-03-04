SET s3_access_key_id = '{{ ACCESS_KEY }}';
SET s3_secret_access_key = '{{ SECRET_KEY }}';
SET s3_endpoint = '{{ ENDPOINT }}';

SELECT count()
FROM read_parquet('s3://{{ BUCKET }}/file.parquet');