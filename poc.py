import boto3
from collections import defaultdict
from pprint import pprint


def autoscaling_groups(instance_map):
    for instance_id, instance_properties in instance_map.items():
        yield instance_properties['AutoScalingGroupName']


def protected_instance_candidates(instance_map):
    for instance_id, instance_properties in instance_map.items():
        if instance_properties['LaunchConfigurationName'] is not None:
            yield instance_id


def main(ecs, autoscaling):
    instance_map = defaultdict(dict)

    # List instances in cluster
    response = ecs.list_container_instances(
        cluster='c1-ci'
    )

    cluster_instance_arns = response['containerInstanceArns']

    # Resolve EC2 instance IDs in cluster
    response = ecs.describe_container_instances(
        cluster='c1-ci',
        containerInstances=cluster_instance_arns
    )

    ec2_instance_ids = [ci['ec2InstanceId'] for ci in response['containerInstances']]

    for ci in response['containerInstances']:
        instance_map[ci['ec2InstanceId']].update({
            'ContainerInstanceArn': ci['containerInstanceArn']
        })

    # Resolve auto-scaling properties for EC2 instance IDs
    response = autoscaling.describe_auto_scaling_instances(
        InstanceIds=ec2_instance_ids
    )

    for i in response['AutoScalingInstances']:
        instance_map[i['InstanceId']].update({
            'LaunchConfigurationName': i.get('LaunchConfigurationName', None),
            'AutoScalingGroupName': i['AutoScalingGroupName']
        })

    response = autoscaling.set_instance_protection(
        InstanceIds=list(protected_instance_candidates(instance_map)),
        AutoScalingGroupName=list(autoscaling_groups(instance_map))[0],
        ProtectedFromScaleIn=False
    )

    #pprint(list(protected_instance_candidates(instance_map)))
    pprint(instance_map)


if __name__ == '__main__':
    autoscaling = boto3.client('autoscaling')
    ecs = boto3.client('ecs')
    main(ecs, autoscaling)

