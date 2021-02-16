import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def greeting(message):
    return {
        "dialogAction": {
            "type": "ElicitIntent",
            "message": {
                "contentType": "PlainText",
                "content": "Hi there, how can I help you?"
            }
        }
    }



def thank_you(message):
    return {
        "dialogAction": {
            "fulfillmentState": 'Fulfilled',
            "type": "Close",
            "message": {
                "contentType": "PlainText",
                "content": "You are welcome! See you next time!"
            }
        }
    }
def dinning(intent_request):
    logger.debug(intent_request)
    if intent_request['invocationSource'] == 'DialogCodeHook':
        return {
            "dialogAction": {
                # "fulfillmentState": 'Fulfilled',
                "type": "ElicitSlot",
                "slots": intent_request['currentIntent']['slots']

            }
        }

    elif intent_request['invocationSource'] == 'FulfillmentCodeHook':
        sqs = boto3.client('sqs')
        slots_collected = intent_request['currentIntent']['slots']
        logger.debug(slots_collected)

        sqs.send_message(
            QueueUrl = "https://sqs.us-east-1.amazonaws.com/110110023967/Q1",
            DelaySeconds= 1 ,
            MessageBody=(
                json.dumps(slots_collected)
            )
        )

        return {
                "dialogAction": {
                    # "fulfillmentState": 'Fulfilled',
                    "type": "ElicitIntent",
                    "message": {
                        "contentType": "PlainText",
                        "content": "Thanks, I have placed your reservation. Expect my recommendations shortly! Have a good day."
                }
            }
        }

def lambda_handler(event, context):

    logger.debug(event)

    intent_name = event['currentIntent']['name']

    if intent_name == "GreetingIntent":
        return greeting(event)
    elif intent_name == 'ThankYouIntent':
        return thank_you(event)
    elif intent_name == 'DiningSuggestionsIntent':
        return dinning(event)

    raise Exception("Intent name {} failed ".format(intent_name)  )
