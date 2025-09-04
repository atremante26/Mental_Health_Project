COMPREHENSIVE_ANALYSIS_PROMPT = """
Analyze the following comprehensive mental health data from multiple sources:

STATISTICAL DATA FROM OFFICIAL SOURCES:
{stats_text}

ONLINE COMMUNITY DISCUSSIONS (REDDIT):
{reddit_text}

Please provide a thorough analysis covering:

1. **Cross-Source Patterns**: How do the statistical trends align or contrast with online community discussions?

2. **Population vs. Individual Perspectives**: What gaps exist between population-level statistics and individual experiences shared online?

3. **Temporal and Demographic Insights**: Are there notable patterns across time periods or demographic groups in the data?

4. **Public Health Implications**: What do these combined data sources suggest about mental health awareness, access to care, and intervention needs?

5. **Data Quality Assessment**: Are there limitations or biases in either data source that should be considered?

6. **Actionable Recommendations**: Based on this comprehensive view, what specific actions could improve mental health outcomes or awareness?

Focus on evidence-based observations from the actual data provided. Use accessible language suitable for public health professionals and general audiences. Avoid speculation beyond what the data directly supports.
"""