import json
import os
from aws_cdk import (
    aws_s3 as s3,
    aws_iam as iam,
    aws_s3_notifications as s3n,
    aws_lambda as lambda_,
    aws_ecr as ecr_,
    aws_ecs as ecs,
    aws_events as events,
    aws_events_targets as targets,
    aws_cloudtrail as trail_,
    aws_ec2 as ec2,
    aws_ssm as ssm,
    core,
)


class S3ToblobStack(core.Stack):
    s3_source_bucket_name = "demo-databucket-source"

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        s3_source, s3_destination = self.s3_buckets()
        s3_batch_role = self.s3_batch_role(s3_source, s3_destination)

        vpc, subnets = self.vpc_network(s3_destination.bucket_arn)
        cluster, task_definition = self.ecs_cluster(
            vpc, s3_destination.bucket_arn)

        fn_create_batch_job, fn_process_transfer_task, fn_create_s3batch_manifest = self.lambda_functions(
            s3_batch_role, s3_destination.bucket_name, cluster.cluster_name, subnets, task_definition)

        self.s3_grant_fn_create_s3batch_manifest(
            s3_source, s3_destination, fn_create_s3batch_manifest)
        s3_destination.add_event_notification(s3.EventType.OBJECT_CREATED,
                                              s3n.LambdaDestination(
                                                  fn_create_s3batch_manifest),
                                              {"prefix": f'{self.s3_source_bucket_name}/demoDataBucketInventory0/', "suffix": '.json'})

        s3_destination.add_event_notification(s3.EventType.OBJECT_CREATED,
                                              s3n.LambdaDestination(
                                                  fn_create_batch_job),
                                              {"prefix": f'csv_manifest/', "suffix": '.csv'})

        self.event_rules(fn_process_transfer_task)
        self.ssm_parameter_store(task_definition.obtain_execution_role())

    def s3_grant_fn_create_s3batch_manifest(self, s3_source, s3_destination, fn_create_s3batch_manifest):
        s3_source.grant_read(fn_create_s3batch_manifest)
        s3_destination.grant_read(fn_create_s3batch_manifest)
        s3_destination.grant_put(fn_create_s3batch_manifest)

    def ssm_parameter_store(self, task_execution_role):
        rg_ssm = ssm.StringParameter(self, "azresourcetoken",
                                     type=ssm.ParameterType.STRING,
                                     parameter_name="/azure/resourcegroup/name",
                                     string_value="dump value")

        rg_ssm.grant_read(task_execution_role)

    def ecs_cluster(self, vpc, bucket_arn):
        ecr = ecr_.Repository(self, "azcopy")
        cluster = ecs.Cluster(self, "DemoCluster",
                              vpc=vpc, container_insights=True)

        task_definition = ecs.FargateTaskDefinition(
            self, "azcopyTaskDef")
        task_definition.add_container("azcopy", image=ecs.ContainerImage.from_registry(
            ecr.repository_uri),
            logging=ecs.LogDrivers.aws_logs(stream_prefix="s32blob"),
            environment={
            'AZURE_BLOB_URL': 'https://mydemostroageaccount.blob.core.windows.net/democontainer/'},
            secrets={
            'SAS_TOKEN': ecs.Secret.from_ssm_parameter(
                ssm.StringParameter.from_secure_string_parameter_attributes(self, 'sas',
                                                                            parameter_name='/azure/storage/sas', version=2))
        })

        task_definition.task_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                bucket_arn,
                f"{bucket_arn}/*"
            ],
            actions=[
                "s3:GetObject",
                "s3:GetObjects",
                "s3:ListObjects",
                "S3:ListBucket"
            ],
        ))
        ecr.grant_pull(task_definition.obtain_execution_role())

        return cluster, task_definition

    def vpc_network(self, bucket_arn):
        vpc = ec2.Vpc(self, "demoVPC",
                      max_azs=2,
                      cidr="10.0.0.0/16",
                      nat_gateways=1,
                      subnet_configuration=[{
                          "cidrMask": 24,
                          "name": 'private',
                          "subnetType": ec2.SubnetType.PRIVATE
                      },
                          {
                          "cidrMask": 24,
                          "name": 'public',
                          "subnetType": ec2.SubnetType.PUBLIC
                      }]
                      )
        subnets = vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE).subnets
        endpoint = vpc.add_gateway_endpoint('s3Endpoint',
                                            service=ec2.GatewayVpcEndpointAwsService.S3,
                                            subnets=[{
                                                "subnet_id": subnets[0].subnet_id
                                            },
                                                {
                                                "subnet_id": subnets[1].subnet_id
                                            }])

        endpoint.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                bucket_arn,
                f"{bucket_arn}/*"
            ],
            principals=[iam.ArnPrincipal("*")],
            actions=[
                "s3:GetObject",
                "s3:GetObjects",
                "s3:ListObjects",
                "S3:ListBucket"
            ],
        ))

        # Provides access to the Amazon S3 bucket containing the layers for each Docker image.
        endpoint.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                f"arn:aws:s3:::prod-{self.region}-starport-layer-bucket/*"
            ],
            principals=[iam.ArnPrincipal("*")],
            actions=[
                "s3:GetObject"
            ],
        ))

        return vpc, subnets

    def s3_buckets(self):
        s3_destination = s3.Bucket(self, "dataBucketInventory",
                                   lifecycle_rules=[
                                       {
                                         'expiration': core.Duration.days(1.0),
                                           'prefix': 'tmp_transition'
                                       },
                                   ])

        s3_source = s3.Bucket(self, "demoDataBucket",
                              bucket_name=self.s3_source_bucket_name,
                              encryption=s3.BucketEncryption.S3_MANAGED,
                              inventories=[
                                  {
                                      "frequency": s3.InventoryFrequency.DAILY,
                                      "include_object_versions": s3.InventoryObjectVersion.CURRENT,
                                      "destination": {
                                          "bucket": s3_destination
                                      }
                                  }
                              ])

        return s3_source, s3_destination

    def s3_batch_role(self, s3_source, s3_destination):
        s3_batch_role = iam.Role(self, "S3BatchRole",
                                 assumed_by=iam.ServicePrincipal(
                                     "batchoperations.s3.amazonaws.com")
                                 )

        s3_batch_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                s3_destination.bucket_arn,
                f"{s3_destination.bucket_arn}/*"
            ],
            actions=[
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:PutObjectTagging",
                "s3:PutObjectLegalHold",
                "s3:PutObjectRetention",
                "s3:GetBucketObjectLockConfiguration"
            ],
        ))

        s3_batch_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                s3_source.bucket_arn,
                f"{s3_source.bucket_arn}/*"
            ],
            actions=[
                "s3:GetObject",
                "s3:GetObjectAcl",
                "s3:GetObjectTagging"
            ],
        ))

        s3_batch_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                f"{s3_destination.bucket_arn}/*"
            ],
            actions=[
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:GetBucketLocation"
            ],
        ))

        s3_batch_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                f"{s3_destination.bucket_arn}/report/{self.s3_source_bucket_name}/*"
            ],
            actions=[
                "s3:PutObject",
                "s3:GetBucketLocation"
            ],
        ))
        return s3_batch_role

    def lambda_functions(self, s3_batch_role, s3_destination_bucket_name, cluster_name, subnets, task_definition):
        fn_create_batch_job = lambda_.Function(self, "CreateS3BatchJobFunction",
                                               runtime=lambda_.Runtime.PYTHON_3_6,
                                               handler="lambda_create_batch_job.handler",
                                               timeout=core.Duration.minutes(
                                                     5),
                                               code=lambda_.Code.from_asset("./src"))
        fn_create_batch_job.add_environment("ROLE_ARN", s3_batch_role.role_arn)
        fn_create_batch_job.add_environment(
            "SOURCE_BUCKET_NAME", self.s3_source_bucket_name)

        fn_create_batch_job.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["s3:CreateJob"],
            resources=["*"]

        ))

        fn_create_batch_job.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["iam:PassRole"],
            resources=[s3_batch_role.role_arn]
        ))

        fn_process_transfer_task = lambda_.Function(self, "ProcessS3TransferFunction",
                                                    runtime=lambda_.Runtime.PYTHON_3_6,
                                                    handler="lambda_process_s3transfer_task.handler",
                                                    timeout=core.Duration.minutes(
                                                        5),
                                                    code=lambda_.Code.from_asset("./src"))
        fn_process_transfer_task.add_environment(
            "CLUSTER_NAME", cluster_name)

        fn_process_transfer_task.add_environment(
            "PRIVATE_SUBNET1", subnets[0].subnet_id)
        fn_process_transfer_task.add_environment(
            "PRIVATE_SUBNET2", subnets[1].subnet_id)
        fn_process_transfer_task.add_environment(
            "TASK_DEFINITION", task_definition.task_definition_arn)
        fn_process_transfer_task.add_environment(
            "S3_BUCKET_NAME", s3_destination_bucket_name)

        fn_process_transfer_task.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                task_definition.task_definition_arn
            ],
            actions=[
                "ecs:RunTask"
            ],
        ))

        fn_process_transfer_task.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["iam:PassRole"],
            resources=[task_definition.execution_role.role_arn]
        ))

        fn_process_transfer_task.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["iam:PassRole"],
            resources=[task_definition.task_role.role_arn]
        ))

        datawranger_layer = lambda_.LayerVersion(self, "DataWrangerLayer",
                                                 code=lambda_.Code.from_asset(
                                                     "./layers/awswrangler-layer-1.9.6-py3.6.zip"),
                                                 compatible_runtimes=[
                                                     lambda_.Runtime.PYTHON_3_6]
                                                 )

        fn_create_s3batch_manifest = lambda_.Function(self, "CreateS3BatchManifest",
                                                      runtime=lambda_.Runtime.PYTHON_3_6,
                                                      handler="lambda_create_s3batch_manifest.handler",
                                                      timeout=core.Duration.minutes(
                                                          15),
                                                      code=lambda_.Code.from_asset(
                                                          "./src"),
                                                      layers=[
                                                          datawranger_layer]
                                                      )

        fn_create_s3batch_manifest.add_environment(
            "DESTINATION_BUCKET_NAME", s3_destination_bucket_name)
        fn_create_s3batch_manifest.add_environment(
            "SOURCE_BUCKET_NAME", self.s3_source_bucket_name)

        fn_create_s3batch_manifest.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                "*"
            ],
            actions=[
                "glue:GetTable",
                "glue:CreateTable",
                "athena:StartQueryExecution",
                "athena:CancelQueryExecution",
                "athena:StopQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults"
            ],
        ))

        fn_create_s3batch_manifest.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[
                f"arn:aws:glue:{self.region}:{self.account}:catalog",
                f"arn:aws:glue:{self.region}:{self.account}:database/*",
                f"arn:aws:glue:{self.region}:{self.account}:table/*"
            ],
            actions=[
                "glue:GetDatabases",
                "glue:GetDatabase",
                "glue:BatchCreatePartition",
                "glue:GetPartitions",
                "glue:CreateDatabase",
                "glue:GetPartition"
            ],
        ))

        return fn_create_batch_job, fn_process_transfer_task, fn_create_s3batch_manifest

    def event_rules(self, fn_process_transfer_task):
        trail = trail_.Trail(
            self, "CloudTrail", send_to_cloud_watch_logs=True)

        event_rule = trail.on_event(self, "S3JobEvent",
                                    target=targets.LambdaFunction(
                                        handler=fn_process_transfer_task)
                                    )
        event_rule.add_event_pattern(
            source=['aws.s3'],
            detail_type=[
                "AWS Service Event via CloudTrail"],
            detail={
                "eventSource": [
                    "s3.amazonaws.com"
                ],
                "eventName": [
                    "JobStatusChanged"
                ],
                "serviceEventDetails": {
                    "status": ["Complete"]
                }
            }
        )
