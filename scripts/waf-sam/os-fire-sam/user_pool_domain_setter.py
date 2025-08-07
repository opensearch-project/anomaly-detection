from __future__ import print_function
import logging
import boto3
import os
import cfnresponse

logger = logging.getLogger(__name__)
client = boto3.client('cognito-idp')


def handler(event, context):
    try:
        logger.info("Got event: %s", event)
        request_type = event['RequestType']

        # Get properties
        stack_name = event['ResourceProperties']['StackName']
        user_pool_id = event['ResourceProperties']['UserPoolId']
        domain = stack_name.lower() + '-' + user_pool_id.replace("_", "-").lower()
        domain = domain.replace("aws", "company")

        if request_type == 'Create':
            logger.info("Creating Resource")
            logger.info("Setting UserPool domain (" + domain + ")")

            client.create_user_pool_domain(
                Domain=domain,
                UserPoolId=user_pool_id,
            )

            cfnresponse.send(event, context, cfnresponse.SUCCESS, {
                "Message": "Domain created successfully",
                "Domain": domain
            })

        elif request_type == 'Update':
            logger.info("Got Update - no action needed")
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {
                "Message": "Update completed",
                "Domain": domain
            })

        elif request_type == 'Delete':
            logger.info("Deleting Resource")

            try:
                client.delete_user_pool_domain(
                    Domain=domain,
                    UserPoolId=user_pool_id
                )
                cfnresponse.send(event, context, cfnresponse.SUCCESS, {
                    "Message": "Domain deleted successfully",
                    "Domain": domain
                })
            except client.exceptions.ResourceNotFoundException:
                # If the domain is already deleted, consider it a success
                logger.info("Domain already deleted")
                cfnresponse.send(event, context, cfnresponse.SUCCESS, {
                    "Message": "Domain was already deleted",
                    "Domain": domain
                })

    except Exception as e:
        logger.error(e)
        cfnresponse.send(event, context, cfnresponse.FAILED, {
            "Message": str(e)
        })
