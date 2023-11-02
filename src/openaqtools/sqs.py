import json
import logging
import polars as pl
import boto3


logger = logging.Logger(__name__)
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(sh)



def messages_to_df(messages: list) -> pl.DataFrame:
    """Converts list of messages to a dataframe

    Args:
        messages (list): messages

    Returns:
        pl.DataFrame: dataframe
    """

    measurements = []
    for message in messages:
        body = json.loads(message["Body"])
        measurement = json.loads(body["Message"])
        measurements.append(measurement)
    
    df = pl.from_dicts(measurements)

    return df


def receive_messages(
    queue_url: str,
    region_name: str,
    max_nr_messages: int = 100000,
    visibility_timeout: int = 1800,
    page_size_messages: int = 10
    ) -> list:
    """Queries for messages on aws sqs

    Args:
        queue_url (str): url of the queue
        region_name (str): region name of the queue
        max_nr_messages (int, optional): Maximum nr of messages to receive. Defaults to 100000.
        visibility_timeout (int, optional): Visibility timeout for received messages. Defaults to 1800.
        page_size_messages (int, optional): Amount of messages to receive per call. Defaults to 10.

    Returns:
        list: received messages
    """

    # define sqs client
    sqs = boto3.client("sqs", region_name=region_name)
    messages = []
    messages_received = 0
    # receive messages until no more messages visible or mx nar messages received
    while messages_received < max_nr_messages:
        logger.warning(f"{messages_received} messages received...")
        # query queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=page_size_messages,
            VisibilityTimeout=visibility_timeout
            )
        
        # add message to message list
        if "Messages" in response:
            current_messages = response["Messages"]
            messages_received += len(current_messages)
            messages.extend(current_messages)
        else:
            logger.warning(f"no messages received...")
            break
    
    return messages