import os
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
load_dotenv()

SLACK_BOT_OAUTH_TOKEN = os.getenv("SLACK_BOT_OAUTH_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL_NAME")

def send_slack_notification(message):
    try:
        client = WebClient(SLACK_BOT_OAUTH_TOKEN)
        client.chat_postMessage(channel=f"#{SLACK_CHANNEL}", text=message)
    except SlackApiError as e:
        print(f"Error sending message: {e}")
