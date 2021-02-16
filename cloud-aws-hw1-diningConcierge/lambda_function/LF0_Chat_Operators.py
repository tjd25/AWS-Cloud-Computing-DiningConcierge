import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
client = boto3.client('lex-runtime')
print(client)

BOT_NAME = "DiningConciergeChatBot"
ERROR_RESP = "Oh no, Something's gone wrong. Please try again later."
BOT_ALIAS = "Prod";

def lambda_handler(event, context):
    logger.debug("event", event)

    input_text = event['messages'][0]['unstructured']['text']
    id = event['messages'][0]['unstructured']['id']
    # logger.debug("input: ", input_text)

    try:
        response = client.post_text(
            botName=BOT_NAME,
            botAlias=BOT_ALIAS,
            userId=id,
            inputText=input_text
        )
        return response

    except ValueError:
        return {
        'statusCode': 400,
        'body': json.dumps(ERROR_RESP)
    }
