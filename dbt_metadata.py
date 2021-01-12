import requests
import json
import config

# ----------------------------------------------------------------------------------
# ----------- FUNCTION TO GET ARRAY OF TABLES AND VIEWS UPDATED BY THE MODEL
#-----------------------------------------------------------------------------------

def get_dbt_catalog(account_id,run_id):

	dbt_catalog_endpoint = "https://cloud.getdbt.com/api/v2/accounts/{}/runs/{}/artifacts/catalog.json".format(account_id,run_id)

	resp = requests.get(
	    url=dbt_catalog_endpoint,
	    headers=config.dbt_headers
	)
	catalog=resp.json()

	#print('Status Code Get Catalog: {}'.format(resp.status_code))
	#print('Result : {}'.format(catalog))

	dbt_catalog=[]

# ------- Loop on nodes of Catalog artifact --------------------------------------

	for node_key in catalog["nodes"]:

		unique_id_array=catalog["nodes"][node_key]["unique_id"].split(".")
		dbt_project_name=unique_id_array[1]
		dbt_model_name=unique_id_array[2]

		bq_object_name=catalog["nodes"][node_key]["metadata"]["name"]
		bq_dataset=catalog["nodes"][node_key]["metadata"]["schema"]
		bq_projet=catalog["nodes"][node_key]["metadata"]["database"]

		artifact_sql_run="https://cloud.getdbt.com/api/v2/accounts/{}/runs/{}/artifacts/run/{}/models/{}.sql".format(account_id,run_id,dbt_project_name,bq_object_name)

		if catalog["nodes"][node_key]["stats"]["has_stats"]["value"]:
			approximate_bytes_size=catalog["nodes"][node_key]["stats"]["num_bytes"]["value"]
			approximate_rows_count=catalog["nodes"][node_key]["stats"]["num_rows"]["value"]
		else:
			approximate_bytes_size="na"
			approximate_rows_count="na"

		dbt_model = {
			"bq_object_name":bq_object_name,
			"bq_projet":bq_projet,
			"bq_dataset":bq_dataset,
			"tag":{
				"approximate_bytes_size":str(approximate_bytes_size),
				"approximate_rows_count":str(approximate_rows_count),
				"dbt_project_name":dbt_project_name,
				"dbt_model_name":dbt_model_name,
				"dbt_sql_run_url":artifact_sql_run
			}
		}

		dbt_catalog.append(dbt_model)

	return dbt_catalog

# ------------------------------------------------------------------------
# ----------- FUNCTION TO GET METADATA FROM THE RUN
#-------------------------------------------------------------------------

def get_dbt_run(account_id,run_id):

# ------- Get dbt Cloud account name --------------------------------------

	dbt_endpoint = "https://cloud.getdbt.com/api/v2/accounts/{}/".format(account_id)
	resp = requests.get(
	    url=dbt_endpoint,
	    headers=config.dbt_headers
	)
	dbt_object=resp.json()
	account_name=dbt_object["data"]["name"]

# ------- Get dbt Cloud run --------------------------------------

	dbt_endpoint = "https://cloud.getdbt.com/api/v2/accounts/{}/runs/{}/".format(account_id,run_id)
	resp = requests.get(
	    url=dbt_endpoint,
	    headers=config.dbt_headers
	)
	dbt_object=resp.json()

	#print('Status Code Get Run: {}'.format(resp.status_code))
	#print('Result : {}'.format(dbt_object))

	project_id=dbt_object["data"]["project_id"]
	job_id=dbt_object["data"]["job_id"]
	run_url=dbt_object["data"]["href"]
	duration=dbt_object["data"]["duration_humanized"]
	run_duration=dbt_object["data"]["run_duration_humanized"]
	run_finished_at=dbt_object["data"]["finished_at"]

# ------- Get dbt Cloud project name --------------------------------------

	dbt_endpoint = "https://cloud.getdbt.com/api/v2/accounts/{}/projects/{}/".format(account_id,project_id)
	resp = requests.get(
	    url=dbt_endpoint,
	    headers=config.dbt_headers
	)
	dbt_object=resp.json()
	project_name=dbt_object["data"]["name"]

# ------- Get dbt Cloud job name --------------------------------------

	dbt_endpoint = "https://cloud.getdbt.com/api/v2/accounts/{}/jobs/{}/".format(account_id,job_id)
	resp = requests.get(
	    url=dbt_endpoint,
	    headers=config.dbt_headers
	)
	dbt_object=resp.json()
	job_name=dbt_object["data"]["name"]

	project_url="https://cloud.getdbt.com/#/accounts/{}/projects/{}/dashboard/".format(account_id,project_id)
	job_url="https://cloud.getdbt.com/#/accounts/{}/projects/{}/jobs/{}/".format(account_id,project_id,job_id)

	dbt_metadata = {
		"dbt_run_id": str(run_id),
		"dbt_run_timestamp":run_finished_at,
		"dbt_duration": duration,
		"dbt_run_duration": run_duration,
		"dbt_run_url": run_url,
		"dbt_job_id": str(job_id),
		"dbt_job_name": job_name,
		"dbt_job_url": job_url,
		"dbt_cloud_project_id": str(project_id),
		"dbt_cloud_project_name": project_name,
		"dbt_cloud_project_url": project_url
	}

	return dbt_metadata