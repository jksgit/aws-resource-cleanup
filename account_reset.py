#!/usr/bin/env python

from __future__ import print_function
import sys
import yaml
import os
import argparse

class Cleaner:

    def __init__(self, config):
        self.config = config

    def _ask(self, question, default="no"):
        valid = {"yes": True, "y": True, "no": False, "n": False}
        if default is None:
            prompt = " [y/n] "
        elif default == "yes":
            prompt = " [Y/n] "
        elif default == "no":
            prompt = " [y/N] "
        else:
            raise ValueError("Invalid default answer: '%s'" % default)
        while True:
            sys.stdout.write(question + prompt)
            choice = raw_input().lower()
            if default is not None and choice == "":
                return valid[default]
            elif choice in valid:
                return valid[choice]
            else:
                sys.stdout.write("Please answer 'yes' or 'no' (or 'y' or 'n').\n")

    global stack_filter
    def stack_filter(output):
        if "RootId" not in output:
            stack = output["StackName"]
            cf.update_termination_protection(EnableTerminationProtection=False, StackName=stack)
            return stack
        else:
            return False

    def _get_deletable_resources(self, describe_function, describe_args, preserve_key, list_key, item_key, filter_function=None):
        if "CloudFormation.Paginator" in str(describe_function.__self__):
             resources = []
             response_iterator = describe_function(**describe_args)
             for page in response_iterator:
                 stack = page[list_key]
                 for output in stack:
                     resources.append(output)
        else:
             resources = describe_function(**describe_args).get(list_key, [])

        preserved_resources = self.config.get("preserved_resources", {}).get(preserve_key, [])
        def can_be_deleted(key, preserved_resources, resource):
            if filter_function:
                return filter_function(resource) and key not in preserved_resources
            else:
                return key not in preserved_resources
        return {resource[item_key]: resource for resource in resources if can_be_deleted(resource[item_key], preserved_resources, resource)}

    def _delete_generic_resource(self, resources, human_name, delete_function, delete_key):
        if resources:
            print("{} that will be deleted:\n".format(human_name), *resources, sep = "\n- ")
            if args.dryrun:
                    exit(0)
            elif self._ask("Delete {}?".format(human_name), "no"):
                for key, resource in resources.iteritems():
                    print("Deleting", key)
                    kwargs = {delete_key: key}
                    delete_function(**kwargs)
        else:
            print("No {} to delete".format(human_name))

    def _simple_delete(self, describe_function, delete_function, preserve_key, list_key, item_key, describe_args={}, filter_function=None):
        deletables = self._get_deletable_resources(describe_function, describe_args, preserve_key, list_key, item_key, filter_function)
        self._delete_generic_resource(deletables, "Stacks", delete_function, item_key)

    def run_safety_checks(self, sts):
        # AWS Account ID in config.yml must match the account we are accessing using an API key
        account_id = sts.get_caller_identity().get("Account")
        assert account_id == self.config.get("assertions").get("account_id"), "Unexpected AWS Account ID, check configuration!"
        print("You are on account {} ".format(account_id))
        if not self._ask("Proceed?", "no"): sys.exit()

    def delete_buckets(self, s3, s3_resource):
        def delete_bucket_and_its_objects(Name):
            bucket = s3_resource.Bucket(Name)
            bucket.object_versions.delete()
            bucket.delete()
        self._simple_delete(s3.list_buckets, delete_bucket_and_its_objects, "s3_buckets", "Buckets", "Name")

    def delete_cloudformation_stacks(self, cf):
        args = {
            "StackStatusFilter": [
                "CREATE_FAILED",
                "CREATE_COMPLETE",
                "ROLLBACK_FAILED",
                "ROLLBACK_COMPLETE",
                "UPDATE_COMPLETE",
                "REVIEW_IN_PROGRESS",
                "UPDATE_ROLLBACK_FAILED",
                "DELETE_FAILED",
                "UPDATE_ROLLBACK_COMPLETE"
            ]
        }
        self._simple_delete(paginator.paginate, cf.delete_stack, "cloudformation", "StackSummaries", "StackName", args, filter_function=stack_filter)

def _get_config_from_file(filename):
    config = {}
    with open(filename, "r") as stream:
        config = yaml.load(stream)
    return config

def get_boto_session():
    import boto3
    return boto3.Session(
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        aws_session_token=os.environ['AWS_SESSION_TOKEN'],
        )

if __name__ == "__main__":
    # initiate the parser
    parser = argparse.ArgumentParser(description = "This script is used to reset an AWS account")
    parser.add_argument("config.yml", help="Configuration file of preserved resources")
    parser.add_argument("-d", "--dryrun", help="Shows the list of resources getting cleaned", action="store_true")
    args = parser.parse_args()

    config = _get_config_from_file(sys.argv[1])
    cleaner = Cleaner(config)
    #print("Current configuration:\n", yaml.dump(config, default_flow_style=False))

    boto_session = get_boto_session()
    sts = boto_session.client("sts")
    cf = boto_session.client("cloudformation")
    paginator = cf.get_paginator('list_stacks')
    s3 = boto_session.client("s3")
    s3_resource = boto_session.resource("s3")
    #cleaner.run_safety_checks(sts)
    #cleaner.delete_buckets(s3, s3_resource)
    cleaner.delete_cloudformation_stacks(cf)
