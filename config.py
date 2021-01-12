
from google.cloud import datacatalog

dbt_auth_token='xxxxxxxxxxxxxxxxxx'
dbt_headers = {"Authorization": "Token "+dbt_auth_token}

dbt_metadata_tag_template_id="dbt_metadata"
dbt_tag_template_project="dbt-test-301310"

datacatalog_client = datacatalog.DataCatalogClient()