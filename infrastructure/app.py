#!/usr/bin/env python3
import aws_cdk as cdk
from ecs_stack import MentalHealthStack

app = cdk.App()
MentalHealthStack(app, "MentalHealthStack")
app.synth()