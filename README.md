# Collection of tools and information of Google cloud platform

## Prepare env
The dependencies are saved in [requirements.txt](gcloud_clients/requirements.txt).

## Run examples in [examples](examples)
```shell
   export GOOGLE_APPLICATION_CREDENTIALS=/path/key.json
   export PYTHONPATH=$PYTHONPATH:$(pwd)
   python examples/bigquery.py
```