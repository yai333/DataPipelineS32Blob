#!/usr/bin/env python3

from aws_cdk import core

from s3toblob.s3toblob_stack import S3ToblobStack


app = core.App()
S3ToblobStack(app, "s3toblob", env={'region': 'ap-southeast-2'})
app.synth()
