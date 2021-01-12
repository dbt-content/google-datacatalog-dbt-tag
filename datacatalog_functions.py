
from google.cloud import datacatalog
from config import datacatalog_client
import config

# ---------------------------------------------------------------
# -------------GET DBT METADATA TAG TEMPLATE  -------------------
# ---------------------------------------------------------------

def get_dbt_tag_template():

	scope = datacatalog.SearchCatalogRequest.Scope()
	scope.include_project_ids.append(config.dbt_tag_template_project)

	tag_templates = datacatalog_client.search_catalog(scope=scope, query='type=tag_template name:'+config.dbt_metadata_tag_template_id)

	for tag_template in tag_templates:
		dbt_metadata_tag_template_name=tag_template.relative_resource_name

	#print('dbt Metadata tag template name : {}'.format(dbt_metadata_tag_template_name))

	return (dbt_metadata_tag_template_name)

# -------------------------------------------------------------
# ------------- GET BIGQUERY ENTRY ID -------------------------
# -------------------------------------------------------------

def get_bq_entry_name(project,dataset,name):

	resource_name = '//bigquery.googleapis.com/projects/{}/datasets/{}/tables/{}'.format(project,dataset,name)
	bq_entry = datacatalog_client.lookup_entry(request={"linked_resource": resource_name})

	#print('BigQuery entry name : {}'.format(bq_entry.name))

	return (bq_entry.name)