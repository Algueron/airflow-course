from airflow.sdk import dag, task, Param
from datetime import datetime

configs = [
    ["dag_a", 100, "2026-01-01T00:00:00", False, "value_2"],
    ["dag_b", 50, "2026-01-01T00:00:00", True, "value_3"]
]

for config in configs:
    @dag(
        dag_id=config[0],
        start_date=datetime(2025, 1, 1),
        schedule="@daily",
        tags=["new"]
    )
    def new_dag(
            extra_input: Param(
                type="integer",
                description="This extra input is used to multiply input_a",
                section="Important Parameters",
                minimum=1,
                maximum=100
            ),
            extra_date: Param(
                default="2025-01-01T00:00:00",
                type="string",
                format="date-time"
            ),
            extra_boolean: Param(
                default=True,
                type="boolean"
            ),
            extra_enum: Param(
                default="value1",
                type="string",
                enum=["value1", "value_2", "value_3"]
            )
    ):

        @task(multiple_outputs=True)
        def extract_data():
            return {
                'input_a': 42,
                'input_b': 43
            }
        
        @task
        def transform_a(input_a, params=None): # The None default value is just to indicate this is a built-in Airflow parameter
            extra_input = params['extra_input']
            return input_a * 2 * extra_input
        
        @task
        def transform_b(input_b):
            return input_b * 3
        
        values = extract_data()
        transform_a(values['input_a'])
        transform_b(values['input_b'])

    new_dag(*config[1:])
