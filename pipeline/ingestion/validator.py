import os
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
            logger.info(f"DAG execution environment:")
            logger.info(f"User: {os.getuid()}")
            logger.info(f"Working directory: {os.getcwd()}")
            logger.info(f"Files readable: {os.access('/opt/airflow/gx/expectations/reddit_suite.json', os.R_OK)}")
            
            context = gx.get_context(project_root_dir='/opt/airflow')
            logger.info(f"Context root directory: {context.root_directory}")
            
            available_suites = context.list_expectation_suite_names()
            logger.info(f"Available suites: {available_suites}")

            suite = context.get_expectation_suite(expectation_suite_name=suite_name)
            
            # Create validator with existing suite
            validator = context.sources.pandas_default.read_dataframe(df)
            validator.expectation_suite = suite
            
            # Run validation
            results = validator.validate()

            # Log which expectations failed
            if not results.success:
                logger.error(f"{suite_name} validation FAILED")
                for result in results.results:
                    if not result.success:
                        logger.error(f"  FAILED: {result.expectation_config.expectation_type}")
                        logger.error(f"    {result.result}")
            else:
                logger.info(f"{suite_name} validation PASSED")
            
            logger.info(f"{suite_name} validation {'PASSED' if results.success else 'FAILED'}")
            return results.success
            
        except Exception as e:
            logger.error(f"Failed to validate {suite_name}: {e}")
            return False