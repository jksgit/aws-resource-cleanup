import boto3
import operator

ACCOUNT = 'XXX'

source = boto3.client('rds', 'us-east-1')
dest = boto3.client('rds', 'us-west-2')
kms = boto3.client('kms', 'us-west-2')

kms_key = kms.describe_key(KeyId='alias/aws/rds')['KeyMetadata']['Arn']

def copy_latest_snapshot():

    response = source.describe_db_snapshots(
        SnapshotType='automated',
        IncludeShared=False,
        IncludePublic=False
    )

    if len(response['DBSnapshots']) == 0:
        raise Exception("No automated snapshots found")

    snapshots_per_project = {}
    for snapshot in response['DBSnapshots']:
        if snapshot['Status'] != 'available':
            continue

        if snapshot['DBInstanceIdentifier'] not in snapshots_per_project.keys():
            snapshots_per_project[snapshot['DBInstanceIdentifier']] = {}

        snapshots_per_project[snapshot['DBInstanceIdentifier']][snapshot['DBSnapshotIdentifier']] = snapshot[
            'SnapshotCreateTime']

    for project in snapshots_per_project:
        sorted_list = sorted(snapshots_per_project[project].items(), key=operator.itemgetter(1), reverse=True)

        copy_name = project + "-" + sorted_list[0][1].strftime("%Y-%m-%d")

        print("Checking if " + copy_name + " is copied")

        try:
            dest.describe_db_snapshots(
                DBSnapshotIdentifier=copy_name
            )
        except:
            response = dest.copy_db_snapshot(
                SourceDBSnapshotIdentifier='arn:aws:rds:us-east-1:' + ACCOUNT + ':snapshot:' + sorted_list[0][0],
                TargetDBSnapshotIdentifier=copy_name,
                KmsKeyId=kms_key,
                SourceRegion='us-east-1',
                CopyTags=True
            )

            if response['DBSnapshot']['Status'] != "pending" and response['DBSnapshot']['Status'] != "available":
                raise Exception("Copy operation for " + copy_name + " failed!")
            print("Copied " + copy_name)

            continue

        print("Already copied")


def remove_old_snapshots():

    response = dest.describe_db_snapshots(
        SnapshotType='manual'
    )

    if len(response['DBSnapshots']) == 0:
        raise Exception("No manual snapshots in Destination")

    snapshots_per_project = {}
    for snapshot in response['DBSnapshots']:
        if snapshot['Status'] != 'available':
            continue

        if snapshot['DBInstanceIdentifier'] not in snapshots_per_project.keys():
            snapshots_per_project[snapshot['DBInstanceIdentifier']] = {}

        snapshots_per_project[snapshot['DBInstanceIdentifier']][snapshot['DBSnapshotIdentifier']] = snapshot[
            'SnapshotCreateTime']

    for project in snapshots_per_project:
        if len(snapshots_per_project[project]) > 1:
            sorted_list = sorted(snapshots_per_project[project].items(), key=operator.itemgetter(1), reverse=True)
            to_remove = [i[0] for i in sorted_list[3:]]

            for snapshot in to_remove:
                print("Removing " + snapshot)
                dest.delete_db_snapshot(
                    DBSnapshotIdentifier=snapshot
                )


def lambda_handler(event, context):
    copy_latest_snapshot()
    remove_old_snapshots()


if __name__ == '__main__':
    lambda_handler(None, None)
