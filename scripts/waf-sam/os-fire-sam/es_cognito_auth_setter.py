from __future__ import print_function
import logging
import boto3
import os
import time
import cfnresponse

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

es = boto3.client('es')


def handler(event, context):
    try:
        logger.info("Got event: %s", event)
        request_type = event['RequestType']
        
        # Get common properties
        properties = event['ResourceProperties']
        stack_name = properties['StackName']
        user_pool_id = properties['UserPoolId']
        identity_pool_id = properties['IdentityPoolId']
        role_arn = properties['RoleArn']
        domain_name = properties['DomainName']
        
        if request_type == 'Create':
            logger.info("Got Create!")
            logger.info("User Pool ID: " + user_pool_id)
            logger.info("Identity Pool ID: " + identity_pool_id)
            logger.info("Role ARN: " + role_arn)
            
            logger.info("Updating ES with Cognito Config!")
            es.update_elasticsearch_domain_config(
                DomainName=domain_name,
                CognitoOptions={
                    'Enabled': True,
                    'UserPoolId': user_pool_id,
                    'IdentityPoolId': identity_pool_id,
                    'RoleArn': role_arn
                }
            )
            
            logger.info("Waiting for ES to be in active state")
            time.sleep(10)
            
            limit = 0
            processing = True
            
            while processing and limit < 20:
                limit += 1
                
                domain_status = es.describe_elasticsearch_domain(
                    DomainName=domain_name
                )['DomainStatus']
                
                processing = domain_status['Processing']
                
                if processing:
                    logger.info(f"Waiting... ES is still processing ({limit})")
                    time.sleep(30)
            
            if limit >= 20:
                logger.warning("Reached maximum wait attempts")
            
            logger.info("Done!")
            
            cfnresponse.send(
                event, 
                context, 
                cfnresponse.SUCCESS, 
                {
                    "Message": "Elasticsearch domain configured successfully",
                    "DomainName": domain_name
                }
            )
            
        elif request_type == 'Update':
            logger.info("Got Update")
            cfnresponse.send(
                event, 
                context, 
                cfnresponse.SUCCESS, 
                {
                    "Message": "Update completed",
                    "DomainName": domain_name
                }
            )
            
        elif request_type == 'Delete':
            logger.info("Got Delete")
            # If you need to clean up Cognito configuration:
            try:
                es.update_elasticsearch_domain_config(
                    DomainName=domain_name,
                    CognitoOptions={
                        'Enabled': False
                    }
                )
            except es.exceptions.ResourceNotFoundException:
                logger.info("Domain already deleted")
            
            cfnresponse.send(
                event, 
                context, 
                cfnresponse.SUCCESS, 
                {
                    "Message": "Delete completed",
                    "DomainName": domain_name
                }
            )
            
    except Exception as e:
        logger.error("Failed to process:", exc_info=True)
        cfnresponse.send(
            event, 
            context, 
            cfnresponse.SUCCESS, 
            {
                "Message": str(e)
            }
        )
