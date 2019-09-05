# Simple GCP clients package for quickly doing simple things
Simplify scripting for simple tasks in the same project with less code.

The clients get basic project information from `GOOGLE_APPLICATION_CREDENTIALS`, so
there is no need to set project information. It is recommended to use service accounts
for scripting, not your own credential. So if there are warnings, **do the right thing**.

## To quickly run
1. `export GOOGLE_APPLICATION_CREDENTIALS=path/key.json`
1. run a demo script (has to be outside the package itself):
   `python -m gcloud_clients.bigquery_c`

## pubsub_to_bq_function
To use this function, run from the [directory](pubsub_to_bq_function):

`gcloud functions deploy pubsub_to_bq --runtime python37 --trigger-topic YOUR_TOPIC --env-vars-file env.yaml`

`env.yaml` has two keys: `dataset` and `table`.

This is a background function, so it can only be invoked by the event source to which they are subscribed.
This means it is securer.

## Note
There is not much of error handlings. Some times you have Python errors, some times you
have gRPC errors.
