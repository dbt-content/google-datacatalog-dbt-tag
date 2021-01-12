from google.cloud import datacatalog
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp
from config import datacatalog_client
import dbt_metadata
import datacatalog_functions
import requests
import json
import config

def dbt_update_datacatalog(request):

	request_json = request.get_json()
	if request_json and 'dbt_run_id' in request_json and 'dbt_account_id' in request_json:
		dbt_run_id = request_json['dbt_run_id']
		dbt_account_id = request_json['dbt_account_id']
	else:
		return 'No dbt Run ID or dbt Account ID for Data Catalog update'

# ---------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------

	dbt_run_metadata = dbt_metadata.get_dbt_run(dbt_account_id,dbt_run_id)
	print('dbt Run Metadata : {}'.format(dbt_run_metadata))

	dbt_catalog = dbt_metadata.get_dbt_catalog(dbt_account_id,dbt_run_id)
	print('dbt Catalog : {}'.format(dbt_catalog))

	dbt_tag_template = datacatalog_functions.get_dbt_tag_template()

	# ----------- Loop on Model found in Catalog and update Data Catalog dbt tag for each table or view ---------------------------

	for model in dbt_catalog:

		# ---------------Get BigQuery entry id for the table or the view ------------------------------

		bq_entry_name = datacatalog_functions.get_bq_entry_name(model["bq_projet"],model["bq_dataset"],model["bq_object_name"])

		# -----------------------------------------------------------------------------------------------
		# ------------- LIST TAGS OF BIGQUERY ENTRY AND SEARCH IF A DBT TAG EXIST -----------------------
		# -----------------------------------------------------------------------------------------------

		dbt_tag_template_found=False
		for entry_tag in datacatalog_client.list_tags(parent=bq_entry_name):

			if entry_tag.template==dbt_tag_template:
				dbt_tag_template_found=True
				dbt_entry_tag_name=entry_tag.name
				break

		# -------------------------------------------------------------------------------
		# ------------- UPDATE OR CREATE A TAG ON ENTRY TABLE OR VIEW -------------------
		# -------------------------------------------------------------------------------

		tag = datacatalog.Tag()
		dbt_run_timestamp = Timestamp()

		# ------------- Tag creation for Run Metadata ----------------------

		for key in dbt_run_metadata:

			value = dbt_run_metadata[key]
			tag_field = datacatalog.TagField()

			if key=="dbt_run_timestamp":
				dbt_run_timestamp.FromDatetime(datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f+00:00"))
				tag_field.timestamp_value = dbt_run_timestamp
			else:
				tag_field.string_value = value
			
			tag.fields[key] = tag_field

		# ------------- Tag creation for Model Metadata ----------------------

		for key in model["tag"]:

			value = model["tag"][key]
			tag_field = datacatalog.TagField()
			tag_field.string_value = value
			tag.fields[key] = tag_field

		if dbt_tag_template_found:

			# ------------- UPDATE AN EXISTING DBT TAG ON THE BIGQUERY ENTRY----------------------
			tag.name=dbt_entry_tag_name
			tag = datacatalog_client.update_tag(tag=tag)

		else:

			# ------------- CREATE A NEW DBTTAG ON THE BIGQUERY ENTRY  ----------------------
			tag.template = dbt_tag_template
			tag=datacatalog_client.create_tag(parent=bq_entry_name, tag=tag)

	return "{} Data Catalog dbt tags updated for run {}".format(len(dbt_catalog),dbt_run_id)

