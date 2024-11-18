from google import pubsub_v1
from google.cloud import storage
import json
import os


client = pubsub_v1.SubscriberClient()
subscription = "projects/speedy-victory-336109/subscriptions/testgcsfile-sub"
storage_client = storage.Client()
bucket = storage_client.bucket('pwm-lowa')
local_path = 'data/'
gcs_path = 'output/'


def pull_msg():
    request = pubsub_v1.PullRequest(
        subscription = subscription,
        max_messages = 1
        # return_immediately = True
    )
    response = client.pull(request=request,)
    return response


def acknowledge(ack_id):
    request = pubsub_v1.AcknowledgeRequest(
        subscription = subscription,
        ack_ids=[ack_id],
    )
    client.acknowledge(request=request)


while True:
    try:
        resp = pull_msg()
        if bool(resp):
            msg = resp.received_messages[0].message.data
            ack_id = resp.received_messages[0].ack_id
            acknowledge(ack_id)
            msg = json.loads(msg.decode('utf-8'))
            uri = msg['name']
            filename = uri.split('/')[-1]
            print(filename)
            # Download file
            blob = bucket.blob(uri)
            blob.download_to_filename(local_path + filename)
            # open file and add 'aaa' in the last line
            with open(local_path + filename, 'a') as f:
                f.write('aaa')
            # upload processed file to gcs
            blob = bucket.blob(gcs_path + filename)
            blob.upload_from_filename(local_path + filename)
            # delete the local file
            os.remove(local_path + filename)
            print('done')

    except Exception as e:
        print(e)
        