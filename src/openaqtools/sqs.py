import json
import logging
import polars as pl
import boto3


logger = logging.Logger(__name__)
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(sh)



def messages_to_df(messages):
    """Parses message data to 

    Args:
        messages (_type_): _description_

    Returns:
        _type_: _description_
    """
    measurements = []
    for message in messages:
        body = json.loads(message["Body"])
        measurement = json.loads(body["Message"])
        measurements.append(measurement)
    
    df = pl.from_dicts(measurements)

    return df


def receive_messages(queue_url, region_name, max_nr_messages=100000, visibility_timeout=1800, page_size_messages=10):
    """_summary_

    Args:
        queue_url (_type_): _description_
        region_name (_type_): _description_
        max_nr_messages (int, optional): _description_. Defaults to 100000.
        visibility_timeout (int, optional): _description_. Defaults to 1800.
        page_size_messages (int, optional): _description_. Defaults to 10.

    Returns:
        _type_: _description_
    """
    sqs = boto3.client("sqs", region_name=region_name)
    messages = []
    messages_received = 0
    while messages_received < max_nr_messages:
        logger.warning(f"{messages_received} messages received...")
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=page_size_messages,
            VisibilityTimeout=visibility_timeout
            )
        
        if "Messages" in response:
            current_messages = response["Messages"]
            messages_received += len(current_messages)
            messages.extend(current_messages)
        else:
            logger.warning(f"no messages received...")
            break
    
    return messages