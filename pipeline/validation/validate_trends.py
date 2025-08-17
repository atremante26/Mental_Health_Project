import logging
import pandas as pd
import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from base_validator import BaseValidator 

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TrendsValidator(BaseValidator):
    def __init__(self):
        super().__init__()
        self.suite_name = "trends_suite"

    def validate(self):
        try:
            # Load data from S3
            response = self.s3.get_object(
                Bucket=self.S3_BUCKET,
                Key=f'trends_processed/trends_processed_{self.today}.json'  
            )
            df = pd.read_json(response['Body'])

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
            validator.expect_table_columns_to_match_ordered_list(["date", "keyword", "interest"])
            validator.expect_column_to_exist("date")
            validator.expect_column_to_exist("keyword")
            validator.expect_column_to_exist("interest")
            validator.expect_column_values_to_not_be_null("date")
            validator.expect_column_values_to_not_be_null("keyword")
            validator.expect_column_values_to_not_be_null("interest")
            validator.expect_column_values_to_be_between("interest", min_value=0, max_value=100)
            validator.expect_column_values_to_match_strftime_format("date", "%Y-%m-%d")
            
            # Save suite
            validator.save_expectation_suite(discard_failed_expectations=False)
            
            # Run validation
            results = validator.validate()
            
            # Build data docs
            context.build_data_docs()
            
            print(f"Validation {'PASSED' if results.success else 'FAILED'}")
            return results.success
        except Exception as e:
            logger.error(f"Failed to load Google Trends processed data from {self.today}: {e}")
            return False
        


        