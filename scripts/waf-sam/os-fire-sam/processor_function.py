import base64
import json


def handler(event, context):
    output = []
    print(event)
    try:
        # Loop through records in incoming Event
        for record in event["records"]:
            # Extract message
            message = json.loads(base64.b64decode(event["records"][0]["data"]))
            
            print('Country: ', message["httpRequest"]["country"])
            print('Action: ', message["action"])
            print('User Agent: ', message["httpRequest"]["headers"][1]["value"])

            timestamp = message["timestamp"]
            action = message["action"]
            country = message["httpRequest"]["country"]
            user_agent = message["httpRequest"]["headers"][1]["value"]
            http_method = message["httpRequest"]["httpMethod"]

            mobile_user_agent, browser_user_agent = filter_user_agent(user_agent)
            us_traffic, uk_traffic, other_traffic = traffic_from_country(
                country, action)
            get_http_method, head_http_method, post_http_method = filter_http_request_method(
                http_method, action)

            # Append new fields in message dict
            message["usTraffic"] = us_traffic
            message["ukTraffic"] = uk_traffic
            message["otherTraffic"] = other_traffic
            message["mobileUserAgent"] = mobile_user_agent
            message["browserUserAgent"] = browser_user_agent
            message["getHttpMethod"] = get_http_method
            message["headHttpMethod"] = head_http_method
            message["postHttpMethod"] = post_http_method

            print("New Message..", message)

            # Base64-encoding
            data = base64.b64encode(json.dumps(message).encode('utf-8'))

            output_record = {
                # Retain same record id from the Kinesis data Firehose
                "recordId": record['recordId'],
                "result": "Ok",
                "data": data.decode('utf-8')
            }
            output.append(output_record)
            
        print(output)
        return {"records": output}
    
    except Exception as e:
        print(e)


def filter_user_agent(user_agent):
    """Returns one hot encoding based on user agent."""
    if "Mobile" in user_agent:
        return (1, 0)
    return (0, 1)  # anomaly recorded


def traffic_from_country(country_code, action):
    """Returns one hot encoding based on allowed traffic from countries."""
    if action == "ALLOW":
        if "US" in country_code:
            return (1, 0, 0)
        elif "UK" in country_code:
            return (0, 1, 0)
        else:
            return (0, 0, 1)  # anomaly recorded
    else:
        return (0, 0, 0)


def filter_http_request_method(http_method, action):
    """Returns one hot encoding based on allowed http method type."""
    if action == "ALLOW":
        if "GET" in http_method:
            return (1, 0, 0)
        elif "HEAD" in http_method:
            return (0, 1, 0)
        elif "POST" in http_method:
            return (0, 0, 1)  # anomaly recorded
    else:
        return (0, 0, 0)
