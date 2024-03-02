import json
import boto3

def lambda_handler(event, context):
    for record in event['Records']:
        message = json.loads(record['Sns']['Message'])
        
        match message['notificationType']:
            case "Bounce":
                handle_bounce(message)
            case "Complaint":
                handle_complaint(message)
            case "Delivery":
                handle_delivery(message)
            case _:
                print(f"Unknown notification type: { message['notificationType'] }")
            
    return {
        'statusCode': 200,
    }


def handle_bounce(message):
    messageId = message['mail']['messageId'];
    addresses = message['bounce']['bouncedRecipients']
    bounceType = message['bounce']['bounceType'];

    print(f"Message { messageId } bounced when sending to { addresses } Bounce type: { bounceType }.");

    for address in addresses:
        writeDDB(address['emailAddress'], message, "disable");


def handle_complaint(message):
    messageId = message['mail']['messageId'];
    addresses = message['complaint']['complainedRecipients']

    print(f"A complaint was reported by { addresses } for message { messageId }.");

    for address in addresses:
        writeDDB(address['emailAddress'], message, "disable");
        


def handle_delivery(message):
    messageId = message['mail']['messageId']
    deliveryTimestamp = message['delivery']['timestamp']
    addresses = message['delivery']['recipients']

    print(f"Message { messageId } was delivered successfully at { deliveryTimestamp }.")

    for address in addresses:
        writeDDB(address, message, "enable");


def writeDDB(id, payload, status):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('mailing')
    
    response = table.put_item(
        Item={
            'UserId': id,
            'notificationType': payload['notificationType'],
            'from': payload['mail']['source'],
            'timestamp': payload['mail']['timestamp'],
            'state': status
        }
    )
    status_code = response['ResponseMetadata']['HTTPStatusCode']
    print(f"statusCode from dynamodn {status_code }")