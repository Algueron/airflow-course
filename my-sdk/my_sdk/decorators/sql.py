from typing import Any, Callable, ClassVar, Collection, Mapping
from collections.abc import Sequence
from airflow.sdk.bases.decorator import DecoratedOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
from airflow.sdk.definitions.context import Context
from airflow.utils.context import context_merge
from airflow.utils.operator_helpers import determine_kwargs
import warnings


class _SQLDecoratedOperator(DecoratedOperator, SQLExecuteQueryOperator):

    # Merges dynamic parameters (such as system date) from both parent operators into this operator
    template_fields: Sequence[str] = (*DecoratedOperator.template_fields, *SQLExecuteQueryOperator.template_fields)
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
        **SQLExecuteQueryOperator.template_fields_renderers
    }

    # Name of the decorator
    custom_operator_name: str = "@task.sql"

    # Internal attribute related to templated fields
    overwrite_rtif_after_execution: bool = True
    
    def __init__(
        self, 
        *, 
        python_callable: Callable, 
        task_id: str, 
        op_args: Collection[Any] | None = None, 
        op_kwargs: Mapping[str, Any] | None = None, 
        kwargs_to_upstream: dict[str, Any] | None = None, 
        **kwargs
    ) -> None:
        
        if kwargs.pop("multiple_outputs", None):
            warnings.warn(
                f"`multiple_outputs=True` is not supported in {self.custom_operator_name} tasks. Ignoring.",
                UserWarning,
                stacklevel=3
            )
        
        super().__init__(
            python_callable=python_callable,
            task_id=task_id,
            op_args=op_args,
            op_kwargs=op_kwargs,
            kwargs_to_upstream=kwargs_to_upstream,
            sql=SET_DURING_EXECUTION,
            multiple_outputs=False,
            **kwargs
        )

    def execute(
        self, 
        context: Context
    ) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        self.sql = self.python_callable(*self.op_args, **kwargs)

        if not isinstance(self.sql, str) or self.sql.strip() == "":
            raise TypeError("The returned value from the TaskFlow callable must be a non-empty string.")
        
        context["ti"].render_templates()

        return super().execute(context)
