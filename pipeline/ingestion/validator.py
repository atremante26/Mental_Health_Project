import logging
import pandas as pd
import great_expectations as gx

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Validator():
    def __init__(self):
        pass 

    def validate(self, df: pd.DataFrame, suite_name: str):
        """Perform data validation with Great Expectations"""
        try:
            # Get context and load existing suite
            context = gx.get_context()
            suite = context.get_expectation_suite(expectation_suite_name=suite_name)
            
            # Create validator with existing suite
            validator = context.sources.pandas_default.read_dataframe(df)
            validator.expectation_suite = suite
            
            # Run validation
            results = validator.validate()
            
            logger.info(f"{suite_name} validation {'PASSED' if results.success else 'FAILED'}")
            return results.success
            
        except Exception as e:
            logger.error(f"Failed to validate {suite_name}: {e}")
            return False