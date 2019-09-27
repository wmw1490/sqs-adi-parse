import json
import sys
import boto3

# lambda_handler--Wally Wilhoite  Demo function for processing ham radio rms trimode logs
# Handler to import adi data to a DynamoDB table
# Source data is sent to this function using SQS from remote client using BOTO3 calls
# Demo function only to show how Lambda scales out based on the number of SQS messages
#
def lambda_handler(event, context):
    # Read message from sqs
    # Create instance of sqs handler    
    client = boto3.client('sqs')

    response = client.receive_message(
        QueueUrl='https://sqs.us-east-2.amazonaws.com/578839498373/sqs-rms-adi-in',
        AttributeNames=[
            'All',
        ],
        MessageAttributeNames=[
            '',
        ],
        MaxNumberOfMessages=1,
        VisibilityTimeout=123,
        WaitTimeSeconds=10,
        ReceiveRequestAttemptId=''
    )
   
    #    f"{QSOdatetime}, {QSObearing}, {QSOcallsign}, {QSOcmsbytes}, {QSOconnectionseconds}, {QSOdistance}, {QSOfreq}, {QSOgridsq}, {QSOlastcommand}, {QSOmode}, {QSOmsgrcv}, {QSOmsgsent}, {QSOradiobytes}"

    body = response.get('Body')
    qsostring = response['Messages'][0]['Body']
    
    qsodatetime, qsobearing, qsocallsign, qsocmsbytes, qsoconnectionseconds, qsodistance, qsofreq, qsogridsq, \
        qsolastcommand, qsomode, qsomsgrcv, qsomsgsent, qsoradiobytes = qsostring.split(',')
    # Connect to DynamoDB
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('QSO')
    
    # Attempt to insert into DynamoDB table
    try:
        table.put_item(Item={'QSOdatetime': qsodatetime, 'QSObearing': qsobearing,'QSOcallsign': qsocallsign, \
                        'QSOconnectionseconds': qsoconnectionseconds, 'QSOdistance': qsodistance, \
                        'QSOfreq': qsofreq, 'QSOgridsq': qsogridsq, 'QSOlastcommand': qsolastcommand, \
                        'QSOmode': qsomode, 'QSOmsgrcv': qsomsgrcv, 'QSOmsgsent': qsomsgsent, 'QSOradiobytes': qsoradiobytes, \
                        'QSOcmsbytes': qsocmsbytes} ) 
    except:
        # do nothing
        print('Unable to write to DynamoDB-20190924 1510')
          
    
    return {
        'statusCode': 200,
        'body': json.dumps('ADI sqs completed')
    }
