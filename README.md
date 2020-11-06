# Product Search Vision API Demo 

## Introduction

The goal of this project is to demonstrate capabilities of Google Cloud [Product Search Vision API](https://cloud.google.com/vision/product-search/docs).

First I'll use a [Kaggle dataset](https://www.kaggle.com/paramaggarwal/fashion-product-images-dataset),
host files on Cloud Storage, use Apache Beam (locally or on Cloud Dataflow) to transform the catalog to a product set
and then call the API to index products.
 
In a second time, with some API call, I'll try to get relevant result with custom images.

## Generate product set

##### Get catalog from Kaggle
You can click on the download button or use the kaggle CLI:

```sh
kaggle datasets download -d paramaggarwal/fashion-product-images-dataset
```

Unzip the archive if needed, go to the folder 

```
gsutil -m cp -r images gs://product_search_vision_api_demo/
gsutil cp styles.csv gs://product_search_vision_api_demo/
```
##### Transform catalog to product set

###### Locally 

```
$ make run
```

###### With Cloud Dataflow

Enable Dataflow API in Cloud Platform Console (replace `project_id` by yours):
```
https://console.developers.google.com/apis/api/dataflow.googleapis.com/overview?project=<project_id>
```

And make sure that your *Application Default Credentials* are valid:
```
gcloud auth application-default login

``` 

Then run:
```
make dataflow
``` 