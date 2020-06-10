"""Single class module implementing methods to interact with slack"""
import mysubprocess as subprocess
# TODO: Replace with proper slack integration using https://github.com/os/slacker
# rather than this home-baked solution


class SlackClient(object):
    """Implements all methods for interacting with slack, namely sending
    messages to advise outcome of provisioning activities"""

    def __init__(self, webhook=None):
        self.webhook = webhook
        import os
        self.hostname = os.uname().nodename

    def send_slack(self, a, b, c, cronname, cronemoji, sendwarnings=None):
        import logging
        import requests
        import json
        log = logging.getLogger('root')

        if a != "":
            slackfailed = {
                "fallback": a.strip("`"),
                "color": "danger",
                "title": "FAILED",
                "text": a,
                "mrkdwn_in": ["text"]
            }
        else:
            slackfailed = ''

        if b != "":
            slackok = {
                "fallback": b.strip("`"),
                "color": "good",
                "title": "COMPLETED",
                "text": b,
                "mrkdwn_in": ["text"]
            }
        else:
            slackok = ''

        if c != "" and sendwarnings:
            slackwarning = {
                "fallback": c.strip("`"),
                "color": "warning",
                "title": "WARNING",
                "text": c,
                "mrkdwn_in": ["text"]
            }
        else:
            slackwarning = ''

        # If there are no new accounts, or failed ones, and warnings aren't on,
        # don't send anything
        if slackfailed == '' and slackok == '' and slackwarning == '':
            if sendwarnings:
                log.info("No messages to be sent to slack! sendwarnings: {}".format(sendwarnings))
            else:
                log.debug("No messages to be sent to slack!")
            return

        # Otherwise, send the message
        log.debug("Slack payload {} {} {}".format(json.dumps(slackfailed),
                                                     json.dumps(slackok),
                                                     json.dumps(slackwarning)))
        slack = {
            "attachments": [slackfailed, slackok, slackwarning],
            "username": cronname+"-"+self.hostname,
            "icon_emoji": ":{}:".format(cronemoji),
            "mrkdwn": "true"
        }

        response = requests.post(
            self.webhook, data=json.dumps(slack),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            log.error(('Request to slack returned an error {}, the response '
                       'is:\n{}').format(response.status_code, response.text))

        return
