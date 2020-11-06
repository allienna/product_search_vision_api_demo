.DEFAULT_GOAL := help
PROJECT_ID=${shell gcloud config get-value project}

BUCKET_NAME=product_search_vision_api_demo
TEMP_LOCATION=gs://${BUCKET_NAME}/temp

CATALOG_DEFINITION_PATH=gs://${BUCKET_NAME}/styles_sample.csv
PRODUCT_SET_IMAGES_PATH=gs://${BUCKET_NAME}/images/
PRODUCT_SET_OUTPUT_PATH=gs://${BUCKET_NAME}/output/product_sets
PRODUCT_SET_ID=product_catalog

test:
	echo ${CATALOG_DEFINITION_PATH}

.PHONY: help
help: ## Print this message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

clean: ## Clean output bucket
	@echo "+ $@"
	@gsutil -m rm -r gs://${BUCKET_NAME}/output/

run: ## Run the program
	@echo "+ $@"
	@python -m main \
           --runner DirectRunner \
           --project ${PROJECT_ID} \
           --region europe-west1 \
           --temp_location ${TEMP_LOCATION} \
           --catalog_definition_path ${CATALOG_DEFINITION_PATH} \
           --product_set_images_path ${PRODUCT_SET_IMAGES_PATH} \
           --product_set_id ${PRODUCT_SET_ID} \
           --product_set_output_path ${PRODUCT_SET_OUTPUT_PATH} \
           --label env=dev \
           --label project=product_search \
           --label owner=aal

dataflow: ## Run the pipeline on Google Cloud Dataflow
	@echo "+ $@"
	@python3 -m main \
         --runner DataflowRunner \
         --project ${PROJECT_ID} \
         --region europe-west1 \
         --temp_location ${TEMP_LOCATION} \
         --catalog_definition_path ${CATALOG_DEFINITION_PATH} \
         --product_set_output_path ${PRODUCT_SET_OUTPUT_PATH} \
         --product_set_images_path ${PRODUCT_SET_IMAGES_PATH} \
         --product_set_id ${PRODUCT_SET_ID} \
         --label env=dev \
         --label project=product_search \
         --label owner=aal \
         --job_name=`git rev-parse --abbrev-ref HEAD|tr '/_' '-'`-`git rev-parse --short HEAD`-`date +%s` \
