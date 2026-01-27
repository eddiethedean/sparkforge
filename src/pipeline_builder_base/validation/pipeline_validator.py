"""
Base pipeline validator with common validation logic.

This module provides a base PipelineValidator class that can be used
by all pipeline builder implementations to validate pipeline configurations.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from ..logging import PipelineLogger
from ..models import PipelineConfig


class PipelineValidator:
    """
    Base pipeline validator with common validation logic.

    This class provides shared validation patterns that can be used
    by all pipeline builder implementations.
    """

    def __init__(self, logger: Optional[PipelineLogger] = None):
        """
        Initialize the pipeline validator.

        Args:
            logger: Optional logger instance for validation messages
        """
        self.logger = logger or PipelineLogger()

    def validate_step_names(
        self,
        bronze_steps: Dict[str, Any],
        silver_steps: Dict[str, Any],
        gold_steps: Dict[str, Any],
    ) -> List[str]:
        """
        Validate that all step names are unique and valid.

        Args:
            bronze_steps: Dictionary of bronze steps
            silver_steps: Dictionary of silver steps
            gold_steps: Dictionary of gold steps

        Returns:
            List of validation errors (empty if valid)
        """
        errors: List[str] = []
        all_step_names: Set[str] = set()

        # Check bronze steps
        for name in bronze_steps.keys():
            if not name or not isinstance(name, str):
                errors.append(f"Invalid bronze step name: {name}")
            elif name in all_step_names:
                errors.append(f"Duplicate step name found: {name}")
            else:
                all_step_names.add(name)

        # Check silver steps
        for name in silver_steps.keys():
            if not name or not isinstance(name, str):
                errors.append(f"Invalid silver step name: {name}")
            elif name in all_step_names:
                errors.append(f"Duplicate step name found: {name}")
            else:
                all_step_names.add(name)

        # Check gold steps
        for name in gold_steps.keys():
            if not name or not isinstance(name, str):
                errors.append(f"Invalid gold step name: {name}")
            elif name in all_step_names:
                errors.append(f"Duplicate step name found: {name}")
            else:
                all_step_names.add(name)

        return errors

    def validate_bronze_steps(
        self,
        bronze_steps: Dict[str, Any],
    ) -> List[str]:
        """
        Validate bronze steps configuration.

        Args:
            bronze_steps: Dictionary of bronze steps

        Returns:
            List of validation errors (empty if valid)
        """
        errors: List[str] = []

        for step_name, step in bronze_steps.items():
            # Check if step has rules
            if not hasattr(step, "rules") or not step.rules:
                errors.append(f"Bronze step '{step_name}' missing validation rules")

            # Check if step has name attribute
            if not hasattr(step, "name") or not step.name:
                errors.append("Bronze step missing name attribute")

        return errors

    def validate_silver_steps(
        self,
        silver_steps: Dict[str, Any],
        bronze_steps: Dict[str, Any],
    ) -> List[str]:
        """
        Validate silver steps configuration and dependencies.

        Args:
            silver_steps: Dictionary of silver steps
            bronze_steps: Dictionary of bronze steps

        Returns:
            List of validation errors (empty if valid)
        """
        errors: List[str] = []

        for step_name, step in silver_steps.items():
            # Skip validation for validation-only steps (existing=True, transform=None)
            existing = getattr(step, "existing", False)
            transform = getattr(step, "transform", None)
            if existing and transform is None:
                # Validation-only step - only check rules and table_name
                if not hasattr(step, "rules") or not step.rules:
                    errors.append(f"Silver step '{step_name}' missing validation rules")
                if not hasattr(step, "table_name") or not step.table_name:
                    errors.append(f"Silver step '{step_name}' missing table_name")
                continue

            # Check if step has source_bronze (for non-validation-only steps)
            source_bronze = getattr(step, "source_bronze", None)
            if not source_bronze:
                errors.append(f"Silver step '{step_name}' missing source_bronze")

            # Check if source_bronze exists in bronze_steps
            elif source_bronze not in bronze_steps:
                errors.append(
                    f"Silver step '{step_name}' depends on non-existent bronze step '{source_bronze}'"
                )

            # Check if step has rules
            if not hasattr(step, "rules") or not step.rules:
                errors.append(f"Silver step '{step_name}' missing validation rules")

            # Check if step has table_name
            if not hasattr(step, "table_name") or not step.table_name:
                errors.append(f"Silver step '{step_name}' missing table_name")

        return errors

    def validate_gold_steps(
        self,
        gold_steps: Dict[str, Any],
        silver_steps: Dict[str, Any],
    ) -> List[str]:
        """
        Validate gold steps configuration and dependencies.

        Args:
            gold_steps: Dictionary of gold steps
            silver_steps: Dictionary of silver steps

        Returns:
            List of validation errors (empty if valid)
        """
        errors: List[str] = []

        for step_name, step in gold_steps.items():
            # Check if step has source_silvers
            source_silvers = getattr(step, "source_silvers", None)
            if source_silvers:
                if not isinstance(source_silvers, list):
                    errors.append(
                        f"Gold step '{step_name}' source_silvers must be a list"
                    )
                else:
                    for silver_name in source_silvers:
                        if silver_name not in silver_steps:
                            errors.append(
                                f"Gold step '{step_name}' depends on non-existent silver step '{silver_name}'"
                            )

            # Check if step has rules
            if not hasattr(step, "rules") or not step.rules:
                errors.append(f"Gold step '{step_name}' missing validation rules")

            # Check if step has table_name
            if not hasattr(step, "table_name") or not step.table_name:
                errors.append(f"Gold step '{step_name}' missing table_name")

        return errors

    def validate_dependencies(
        self,
        bronze_steps: Dict[str, Any],
        silver_steps: Dict[str, Any],
        gold_steps: Dict[str, Any],
    ) -> List[str]:
        """
        Validate step dependencies and detect circular dependencies.

        Args:
            bronze_steps: Dictionary of bronze steps
            silver_steps: Dictionary of silver steps
            gold_steps: Dictionary of gold steps

        Returns:
            List of validation errors (empty if valid)
        """
        errors: List[str] = []

        # Build dependency graph
        dependencies: Dict[str, List[str]] = {}

        # Silver steps depend on bronze steps
        for step_name, step in silver_steps.items():
            source_bronze = getattr(step, "source_bronze", None)
            if source_bronze:
                dependencies[step_name] = [source_bronze]

        # Gold steps depend on silver steps
        for step_name, step in gold_steps.items():
            source_silvers = getattr(step, "source_silvers", None)
            if source_silvers and isinstance(source_silvers, list):
                dependencies[step_name] = source_silvers

        # Check for circular dependencies using DFS
        visited: Set[str] = set()
        rec_stack: Set[str] = set()

        def has_cycle(node: str) -> bool:
            """Check if there's a cycle starting from node."""
            visited.add(node)
            rec_stack.add(node)

            for neighbor in dependencies.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    # Found a back edge, cycle exists
                    return True

            rec_stack.remove(node)
            return False

        # Check all nodes for cycles
        all_nodes = set(dependencies.keys())
        for node in all_nodes:
            if node not in visited:
                if has_cycle(node):
                    errors.append(
                        f"Circular dependency detected involving step '{node}'"
                    )

        return errors

    def validate_schema(self, schema: Any) -> List[str]:
        """
        Validate schema name format.

        Args:
            schema: Schema name to validate

        Returns:
            List of validation errors (empty if valid)
        """
        errors: List[str] = []

        if not schema:
            errors.append("Schema name cannot be empty")
        elif not isinstance(schema, str):
            errors.append("Schema name must be a string")
        elif not schema.strip():
            errors.append("Schema name cannot be whitespace only")
        elif len(schema) > 128:  # Reasonable limit
            errors.append("Schema name is too long (max 128 characters)")

        return errors

    def validate_pipeline(
        self,
        config: PipelineConfig,
        bronze_steps: Dict[str, Any],
        silver_steps: Dict[str, Any],
        gold_steps: Dict[str, Any],
    ) -> List[str]:
        """
        Validate entire pipeline configuration.

        Args:
            config: Pipeline configuration
            bronze_steps: Dictionary of bronze steps
            silver_steps: Dictionary of silver steps
            gold_steps: Dictionary of gold steps

        Returns:
            List of validation errors (empty if valid)
        """
        errors: List[str] = []

        # Validate schema
        schema_errors = self.validate_schema(config.schema)
        errors.extend(schema_errors)

        # Validate step names
        name_errors = self.validate_step_names(bronze_steps, silver_steps, gold_steps)
        errors.extend(name_errors)

        # Validate bronze steps
        bronze_errors = self.validate_bronze_steps(bronze_steps)
        errors.extend(bronze_errors)

        # Validate silver steps
        silver_errors = self.validate_silver_steps(silver_steps, bronze_steps)
        errors.extend(silver_errors)

        # Validate gold steps
        gold_errors = self.validate_gold_steps(gold_steps, silver_steps)
        errors.extend(gold_errors)

        # Validate dependencies
        dep_errors = self.validate_dependencies(bronze_steps, silver_steps, gold_steps)
        errors.extend(dep_errors)

        return errors
