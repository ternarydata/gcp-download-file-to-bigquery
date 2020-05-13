import os
import wget

from google.cloud import storage

url = os.environ['URL']
bucket_name = os.environ['BUCKET'] #without gs://
file_name = os.environ['FILE_NAME']

cf_path = '/tmp/{}'.format(file_name)

def import_file(event, context):

	# set storage client
	client = storage.Client()

	# get bucket
	bucket = client.get_bucket(bucket_name)

	# download the file to Cloud Function's tmp directory
	wget.download(url, cf_path)

	# set Blob
	blob = storage.Blob(file_name, bucket)
 
	# upload the file to GCS
	blob.upload_from_filename(cf_path)

	print("""This Function was triggered by messageId {} published at {}
    """.format(context.event_id, context.timestamp))