from __future__ import print_function
import cfnresponse
import logging
import boto3
import os
from datetime import datetime
from urllib.parse import urlparse
import hashlib
import hmac
import json
import urllib3
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.credentials import Credentials

# Configure logging
logger = logging.getLogger(__name__)

# Initialize AWS clients
waf = boto3.client('waf')
wafRegional = boto3.client('waf-regional')
wafv2_cloudfront = boto3.client('wafv2', region_name='us-east-1')
wafv2_regional = boto3.client('wafv2')

  # Initialize HTTP client
http = urllib3.PoolManager()

def make_request(method, url, auth=None, headers=None, body=None):
        """Make HTTP request using urllib3"""
        if headers is None:
            headers = {}

        if auth:
            credentials = boto3.Session().get_credentials()
            request = AWSRequest(method=method, url=url,
                                 data=body, headers=headers)
            SigV4Auth(credentials, 'es', auth.region).add_auth(request)
            headers.update(dict(request.headers))

        try:
            response = http.request(
                method,
                url,
                headers=headers,
                body=body
            )
            return response
        except Exception as e:
            logger.error(f"Request failed: {str(e)}")
            raise

class AWS4Auth:
        """AWS4 authentication class for signing AWS requests"""

        def __init__(self, access_key, secret_key, region, service, session_token=None):
            """Initialize with AWS credentials and request information"""
            self.access_key = access_key
            self.secret_key = secret_key
            self.region = region
            self.service = service
            self.session_token = session_token

        def __call__(self, request):
            """Sign the request with AWS4 authentication"""
            # Generate timestamp information
            t = datetime.utcnow()
            amzdate = t.strftime('%Y%m%dT%H%M%SZ')
            datestamp = t.strftime('%Y%m%d')
            credential_scope = f"{datestamp}/{self.region}/{self.service}/aws4_request"

            # Parse request URL
            url_parts = urlparse(request.url)
            host = url_parts.netloc
            canonical_uri = url_parts.path if url_parts.path else '/'

            # Build canonical headers
            canonical_headers = f"host:{host}\nx-amz-date:{amzdate}\n"
            signed_headers = "host;x-amz-date"

            # Add session token if present
            if self.session_token:
                canonical_headers += f"x-amz-security-token:{self.session_token}\n"
                signed_headers += ";x-amz-security-token"

            # Create canonical request
            algorithm = "AWS4-HMAC-SHA256"
            canonical_querystring = url_parts.query
            payload_hash = hashlib.sha256(
                (request.body or "").encode('utf-8')).hexdigest()

            canonical_request = (
                f"{request.method}\n"
                f"{canonical_uri}\n"
                f"{canonical_querystring}\n"
                f"{canonical_headers}\n"
                f"{signed_headers}\n"
                f"{payload_hash}"
            )

            # Create string to sign
            string_to_sign = (
                f"{algorithm}\n"
                f"{amzdate}\n"
                f"{credential_scope}\n"
                f"{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
            )

            # Calculate signature
            def sign(key, msg):
                return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

            k_date = sign(f"AWS4{self.secret_key}".encode('utf-8'), datestamp)
            k_region = sign(k_date, self.region)
            k_service = sign(k_region, self.service)
            k_signing = sign(k_service, "aws4_request")
            signature = hmac.new(
                k_signing,
                string_to_sign.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()

            # Create authorization header
            authorization_header = (
                f"{algorithm} "
                f"Credential={self.access_key}/{credential_scope}, "
                f"SignedHeaders={signed_headers}, "
                f"Signature={signature}"
            )

            # Update request headers
            request.headers.update({
                'x-amz-date': amzdate,
                'Authorization': authorization_header
            })

            if self.session_token:
                request.headers['x-amz-security-token'] = self.session_token

            return request

def handler(event, context):
        """Lambda handler function"""
        try:
            logger.info('Received event: %s', event)

            if event['RequestType'] in ['Create', 'Update']:
                region = event['ResourceProperties']['Region']
                host = event['ResourceProperties']['Host']
                accountId = event['ResourceProperties']['AccountID']

                service = 'es'
                credentials = boto3.Session().get_credentials()
                awsauth = AWS4Auth(
                    credentials.access_key,
                    credentials.secret_key,
                    region,
                    service,
                    session_token=credentials.token
                )

                import_index_template(host, awsauth)
                update_all(host, awsauth)

                cfnresponse.send(event, context, cfnresponse.SUCCESS, {
                    'Message': 'Resource creation successful!'
                })

            elif event['RequestType'] == 'Delete':
                # Handle deletion if needed
                logger.info('Delete request - no action needed')
                cfnresponse.send(event, context, cfnresponse.SUCCESS, {
                    'Message': 'Resource deletion successful!'
                })

        except Exception as e:
            logger.error('Error: %s', str(e))
            cfnresponse.send(event, context, cfnresponse.FAILED, {
                'Error': str(e)
            })

def import_index_template(host, awsauth):
        url = 'https://' + host + '/_template/awswaf-logs'
        headers = {"Content-Type": "application/json"}

        with open("custom/template.json") as f:
            template = f.read()

        make_request("PUT", url, awsauth, headers, template)

def import_kibana_object(host, awsauth, type, name):
        url = 'https://' + host + '/_plugin/kibana/api/saved_objects/' + type + '/' + name
        headers = {
            "Content-Type": "application/json",
            'kbn-xsrf': 'true'
        }

        with open("custom/" + name + ".json") as f:
            template = f.read()

        res = make_request("POST", url, awsauth, headers, template)
        print(res.data.decode('utf-8'))

def delete_kibana_object(host, awsauth, type, name):
        url = 'https://' + host + '/_plugin/kibana/api/saved_objects/' + type + '/' + name
        headers = {'kbn-xsrf': 'true'}
        response = make_request("DELETE", url, awsauth, headers)
        print(response.data.decode('utf-8'))

def import_kibana_index_pattern(host, awsauth, type, name):
        url = 'https://' + host + '/_plugin/kibana/api/saved_objects/' + type + '/' + name
        headers = {
            "Content-Type": "application/json",
            'kbn-xsrf': 'true'
        }

        with open("custom/" + name + ".json") as f:
            template = f.read()

        webacls_mapping = generate_wafacls_mapping()
        template = template.replace("WEBACL_CUSTOM_MAPPINGS", webacls_mapping)

        rules_mapping = generate_rules_mapping()
        template = template.replace("RULE_CUSTOM_MAPPINGS", rules_mapping)

        response = make_request("POST", url, awsauth, headers, template)

def generate_rules_mapping():
        mappings = ""

        rules = wafRegional.list_rules()["Rules"]
        for rule in rules:
            mapping = f'if (rule == \\"{rule["RuleId"]}\\") {{ return \\"{rule["Name"]}\\";}}\\n'
            mappings += mapping

        rules = waf.list_rules()["Rules"]
        for rule in rules:
            mapping = f'if (rule == \\"{rule["RuleId"]}\\") {{ return \\"{rule["Name"]}\\";}}\\n'
            mappings += mapping

        return mappings

def update_all(host, awsauth):
        delete_kibana_object(host, awsauth, "index-pattern", "awswaf")
        import_kibana_index_pattern(host, awsauth, "index-pattern", "awswaf")

        visualizations = [
            "allcountries", "executedrules", "filters", "numberofallrequests",
            "numberofblockedrequests", "allvsblockedrequests", "top10countries",
            "top10useragents", "top10uris", "top10rules", "top10ip", "top10hosts",
            "httpmethods", "httpversions", "uniqueipcount", "requestcount", "top10webacl"
        ]

        for viz in visualizations:
            import_kibana_object(host, awsauth, "visualization", viz)

        import_kibana_object(host, awsauth, "dashboard", "dashboard")

def generate_wafacls_mapping():
        mappings = ""

        # Regional WebACLs
        webacls = wafRegional.list_web_acls()["WebACLs"]
        for webacl in webacls:
            mapping = f'if (webacl == \\"{webacl["WebACLId"]}\\") {{ return \\"{webacl["Name"]}\\";}}\\n'
            mappings += mapping

        # Global WebACLs
        webacls = waf.list_web_acls()["WebACLs"]
        for webacl in webacls:
            mapping = f'if (webacl == \\"{webacl["WebACLId"]}\\") {{ return \\"{webacl["Name"]}\\";}}\\n'
            mappings += mapping

        # CloudFront WebACLs
        webacls = wafv2_cloudfront.list_web_acls(Scope='CLOUDFRONT')['WebACLs']
        for webacl in webacls:
            logWebACLId = f"arn:aws:wafv2:us-east-1:{os.environ['ACCOUNT_ID']}:global/webacl/{webacl['Name']}/{webacl['Id']}"
            mapping = f'if (webacl == \\"{logWebACLId}\\") {{ return \\"{webacl["Name"]}\\";}}\\n'
            mappings += mapping

        # Regional V2 WebACLs
        webacls = wafv2_regional.list_web_acls(Scope='REGIONAL')['WebACLs']
        for webacl in webacls:
            logWebACLId = f"arn:aws:wafv2:{os.environ['REGION']}:{os.environ['ACCOUNT_ID']}:regional/webacl/{webacl['Name']}/{webacl['Id']}"
            mapping = f'if (webacl == \\"{logWebACLId}\\") {{ return \\"{webacl["Name"]}\\";}}\\n'
            mappings += mapping

        return mappings

def update_kibana(event, context):
        region = os.environ['REGION']
        host = os.environ['ES_ENDPOINT']
        service = 'es'
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            region,
            service,
            session_token=credentials.token
        )

        delete_kibana_object(host, awsauth, "index-pattern", "awswaf")
        import_kibana_index_pattern(host, awsauth, "index-pattern", "awswaf")

        # Commented out for now
        # update_all(host, awsauth)
