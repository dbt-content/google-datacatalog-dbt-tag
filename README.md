# dbt and Cloud dbt Tags

<img src="https://github.com/dbt-content/google-datacatalog-dbt-tag/raw/main/images/dbt_datacatalog.png" width="80%" height="80%">

Create or update [Google Cloud Data Catalog](https://cloud.google.com/data-catalog/) tags on BigQuery tables with [Cloud dbt](https://cloud.getdbt.com/) Metadata via a [Cloud Function](https://cloud.google.com/functions).

Data Catalog tag created or updated:
**[dbt Run Metadata](https://github.com/dbt-content/google-datacatalog-dbt-tag/blob/main/tag_templates/dbt_metadata_tag_template.txt)** tag attached to the BigQuery table and containing information from the dbt Run used to create or update the BigQuery table : the user, dbt Job (id, name, url, timestamp), dbt Dataset (id, name, url), dbt Flow (id, name, url), Job Profile (url and nb valid, invalid an empty values) and the Dataflow job (id, url).

To activate, learn and use Cloud Data Catalog, go to [https://cloud.google.com/data-catalog](https://cloud.google.com/data-catalog) and [https://console.cloud.google.com/datacatalog](https://console.cloud.google.com/datacatalog).

This repository contains the Cloud Function Python code to create or update the Data Catalog tag.

This Cloud Function uses:
- [Python Client for Google Cloud Data Catalog API](https://googleapis.dev/python/datacatalog/latest/index.html#)
- [Cloud dbt REST API](https://docs.getdbt.com/dbt-cloud/api/)

In your Cloud Function, you need the 5 files:
- [main.py](https://github.com/dbt-content/google-datacatalog-dbt-tag/blob/main/main.py)
- [config.py](https://github.com/dbt-content/google-datacatalog-dbt-tag/blob/main/config.py) where you need to update your **GCP project name** (where Tags Template are created) and the **[dbt Access Token](https://docs.trifacta.com/display/DP/Access+Tokens+Page)** (to use dbt API). You can also update the 2 tag templates ID if needed.
- [datacatalog_functions.py](https://github.com/dbt-content/google-datacatalog-dbt-tag/blob/main/datacatalog_functions.py)
- [dbt_metadata.py](https://github.com/dbt-content/google-datacatalog-dbt-tag/blob/main/dbt_metadata.py)
- [requirements.txt](https://github.com/dbt-content/google-datacatalog-dbt-tag/blob/main/requirements.txt)


Before runing the Cloud Function (and create or update tags), you need to create the a Data Catalog Tag Template for dbt ([Run Metadata](https://github.com/dbt-content/google-datacatalog-dbt-tag/blob/main/tag_templates/dbt_metadata_tag_template.txt).

You can use:

- **Cloud Console** where you can [manage your Tag Templates](https://console.cloud.google.com/datacatalog?q=type%3DTAG_TEMPLATE)

- **gcloud** and the command `gcloud data-catalog tag-templates create`, full command lines in [gcloud_tag-templates_create.sh](https://github.com/dbt-content/google-datacatalog-dbt-tag/blob/main/tag_templates/gcloud_tag-templates_create.sh), more details with and [example](https://cloud.google.com/data-catalog/docs/quickstart-tagging#data-catalog-quickstart-gcloud) and [reference](https://cloud.google.com/sdk/gcloud/reference/data-catalog/tag-templates/create). But be aware that with gcloud command line, you cannot manage template tag fields's order, fields will be in alphabetical order.

- **REST API** with the tag template json file [dbt_metadata_tag_template.json](https://github.com/dbt-content/google-datacatalog-dbt-tag/blob/main/tag_templates/dbt_metadata_tag_template.json), more details with an [example](https://cloud.google.com/data-catalog/docs/quickstart-tagging#data-catalog-quickstart-drest) and [reference](https://cloud.google.com/data-catalog/docs/reference/rest/v1/projects.locations.tagTemplates/create).

To use the Cloud Function you just have to pass the dbt Cloud Run ID and the dbt Cloud Account ID in a JSON format like ```{"job_id":"7827359"}```.

When Data Catalog template tags are created and when tags are created or updated on BigQuery tables, you can find all results from [https://console.cloud.google.com/datacatalog](https://console.cloud.google.com/datacatalog).


Finally, you can also search BigQuery tables in Cloud Data Catalog with a dbt tag from your own application like [https://github.com/victorcouste/dbt-datacatalog-explorer](https://github.com/dbt-content/dbt-datacatalog-explorer)

<br>
Happy tagging !
<br><br><br>

![image](images/DataCatalog_dbt_metadata_tag.png)

![image](images/DataCatalog_dbt_column_profile_tag.png)

![image](images/DataCatalog_dbt_metadata_tag_template.png)

![image](images/DataCatalog_dbt_column_profile_tag_template.png)

![image](images/Google_Cloud_Data_Catalog_dbt_Webhook.png)
