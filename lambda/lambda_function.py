import json
from datetime import datetime
import boto3

ddb = boto3.client('dynamodb')
tablename = "openaqtest"
partitionkey = "parameter"
sortkey = "timestamp"


def lambda_handler(event, context):
    
    for record in event["Records"]:
        body = json.loads(record["body"])
        message = json.loads(body["Message"])
        print(message)
        dt = datetime.strptime(message["date"]["utc"], '%Y-%m-%d %H:%M:%S.%f')

        Item = {
            partitionkey: {"S": message["parameter"]},
            sortkey: {"N": str(dt.timestamp())},
            "location": {"S": message["location"]},
            "value": {"N": str(message["value"])},
            "coordinates": {"S": json.dumps(message["coordinates"])},
            "date": {"S": json.dumps(message["date"])},
            "attributes": {"S": json.dumps({key: message[key] for key in ["unit", "country", "isMobile", "isAnalysis", "sensorType", "entity"]})}
            }
        
        print(Item)
        
        ddb.put_item(TableName=tablename, Item=Item)
 
    
    return {
        'statusCode': 200,
        'body': json.dumps("Success")
    }
