import logging
import pandas as pd
import great_expectations as gx
from base_validator import BaseValidator 

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedditValidator(BaseValidator):
    def __init__(self):
        super().__init__()
        self.suite_name = "reddit_suite"

    def validate(self):
        try:
            # Load data from S3
            response = self.s3.get_object(
                Bucket=self.S3_BUCKET,
                Key=f'reddit-processed/reddit_processed_{self.today}.json'  
            )
            df = pd.read_json(response['Body'])
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

            # Get context
            context = gx.get_context()
            
            # Create or get expectation suite
            try:
                suite = context.get_expectation_suite(expectation_suite_name=self.suite_name)
                print(f'Loaded ExpectationSuite "{suite.expectation_suite_name}"')
            except:
                suite = context.add_expectation_suite(expectation_suite_name=self.suite_name)
                print(f'Created ExpectationSuite "{suite.expectation_suite_name}"')
            
            # Create validator
            validator = context.sources.pandas_default.read_dataframe(df)
            validator.expectation_suite_name = self.suite_name
            
            # Define expectations
            validator.expect_table_columns_to_match_ordered_list(["subreddit", "date", "title", "text", "score", "comments"])

            validator.expect_column_to_exist("subreddit")
            validator.expect_column_to_exist("date") 
            validator.expect_column_to_exist("title")
            validator.expect_column_to_exist("text")
            validator.expect_column_to_exist("score")
            validator.expect_column_to_exist("comments")

            validator.expect_column_values_to_not_be_null("subreddit")
            validator.expect_column_values_to_not_be_null("date") 
            validator.expect_column_values_to_not_be_null("title")
            validator.expect_column_values_to_not_be_null("text")
            validator.expect_column_values_to_not_be_null("score")
            validator.expect_column_values_to_not_be_null("comments")

            validator.expect_column_values_to_match_strftime_format("date", "%Y-%m-%d")

            validator.expect_column_values_to_be_of_type("score", "int64")
            validator.expect_column_values_to_be_of_type("comments", "int64")

            validator.expect_column_values_to_be_between("score", min_value=0)  
            validator.expect_column_values_to_be_between("comments", min_value=0)

            validator.expect_column_values_to_be_in_set("subreddit", ["mentalhealth", "depression", "anxiety"])
            
            # Save suite
            validator.save_expectation_suite(discard_failed_expectations=False)
            
            # Run validation
            results = validator.validate()
            
            # Build data docs
            context.build_data_docs()
            
            print(f"Validation {'PASSED' if results.success else 'FAILED'}")
            return results.success
        except Exception as e:
            logger.error(f"Failed to load Reddit processed data from {self.today}: {e}")
            return False