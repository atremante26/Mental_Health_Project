REDDIT_SENTIMENT_PROMPT = """
Analyze the following mental health Reddit posts and provide insights:

Posts:
{posts_text}

Please provide:
1. Overall sentiment trends
2. Common themes and concerns
3. Language patterns that indicate distress levels
4. Emerging topics or changes over time

Focus on actionable insights for mental health awareness. Avoid diagnostic language.
"""

STATISTICAL_SUMMARY_PROMPT = """
Based on the following mental health statistics, generate a clear summary:

Data:
{statistics}

Please provide:
1. Key trends and patterns
2. Notable changes or anomalies
3. Contextual interpretation
4. Implications for mental health awareness

Use accessible language suitable for general audiences.
"""

CLUSTER_INTERPRETATION_PROMPT = """
Interpret these mental health survey clusters:

Cluster Data:
{cluster_profiles}

Please provide:
1. Meaningful names for each cluster
2. Key characteristics of each group
3. Potential intervention strategies
4. Limitations of this segmentation approach

Be careful not to stereotype or oversimplify mental health experiences.
"""