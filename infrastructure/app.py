import os
import aws_cdk as cdk
from ecs_stack import MentalHealthStack
from dotenv import load_dotenv

# Load AWS environment variables
load_dotenv(dotenv_path="../.env")

app = cdk.App()

MentalHealthStack(app, "MentalHealthStack", 
    env=cdk.Environment(
        account=os.getenv('AWS_ACCOUNT_ID'),
        region=os.getenv('AWS_DEFAULT_REGION')
    )
)

app.synth()