"""
Pipeline validation system for SparkForge.

This module provides comprehensive validation for pipeline configurations,
step definitions, and execution contexts.
"""

from __future__ import annotations
from typing import Dict, List, Optional, Any, Protocol
from dataclasses import dataclass

from .models import PipelineConfig
from ..models import BronzeStep, SilverStep, GoldStep, ExecutionContext
from ..logger import PipelineLogger


class StepValidator(Protocol):
    """Protocol for custom step validators."""
    
    def validate(self, step: Any, context: ExecutionContext) -> List[str]:
        """Validate a step and return any validation errors."""
        ...


@dataclass
class ValidationResult:
    """Result of pipeline validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    recommendations: List[str]
    
    def __bool__(self) -> bool:
        """Return whether validation passed."""
        return self.is_valid


class PipelineValidator:
    """
    Comprehensive pipeline validation system.
    
    This class provides validation for pipeline configurations, step definitions,
    and execution contexts, ensuring data quality and preventing runtime errors.
    
    Features:
    - Pipeline configuration validation
    - Step definition validation
    - Dependency validation
    - Data quality threshold validation
    - Custom validator support
    """
    
    def __init__(self, logger: Optional[PipelineLogger] = None):
        self.logger = logger or PipelineLogger()
        self.custom_validators: List[StepValidator] = []
    
    def add_validator(self, validator: StepValidator) -> None:
        """Add a custom step validator."""
        self.custom_validators.append(validator)
        self.logger.info(f"Added custom validator: {validator.__class__.__name__}")
    
    def validate_pipeline(
        self,
        config: PipelineConfig,
        bronze_steps: Dict[str, BronzeStep],
        silver_steps: Dict[str, SilverStep],
        gold_steps: Dict[str, GoldStep]
    ) -> ValidationResult:
        """
        Validate the entire pipeline configuration.
        
        Args:
            config: Pipeline configuration
            bronze_steps: Bronze step definitions
            silver_steps: Silver step definitions
            gold_steps: Gold step definitions
            
        Returns:
            ValidationResult containing validation status and issues
        """
        errors: List[str] = []
        warnings: List[str] = []
        recommendations: List[str] = []
        
        # Validate configuration
        config_errors = self._validate_config(config)
        errors.extend(config_errors)
        
        # Validate bronze steps
        bronze_errors, bronze_warnings = self._validate_bronze_steps(bronze_steps)
        errors.extend(bronze_errors)
        warnings.extend(bronze_warnings)
        
        # Validate silver steps
        silver_errors, silver_warnings = self._validate_silver_steps(silver_steps, bronze_steps)
        errors.extend(silver_errors)
        warnings.extend(silver_warnings)
        
        # Validate gold steps
        gold_errors, gold_warnings = self._validate_gold_steps(gold_steps, silver_steps)
        errors.extend(gold_errors)
        warnings.extend(gold_warnings)
        
        # Validate dependencies
        dep_errors, dep_warnings = self._validate_dependencies(bronze_steps, silver_steps, gold_steps)
        errors.extend(dep_errors)
        warnings.extend(dep_warnings)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(config, bronze_steps, silver_steps, gold_steps)
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Pipeline validation passed")
        else:
            self.logger.error(f"Pipeline validation failed with {len(errors)} errors")
        
        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            recommendations=recommendations
        )
    
    def validate_step(
        self,
        step: Any,
        step_type: str,
        context: ExecutionContext
    ) -> ValidationResult:
        """Validate a single step."""
        errors: List[str] = []
        warnings: List[str] = []
        
        # Run custom validators
        for validator in self.custom_validators:
            try:
                validator_errors = validator.validate(step, context)
                errors.extend(validator_errors)
            except Exception as e:
                errors.append(f"Validator {validator.__class__.__name__} failed: {str(e)}")
        
        # Basic step validation
        if hasattr(step, 'name') and not step.name:
            errors.append(f"{step_type} step must have a name")
        
        if hasattr(step, 'rules') and not step.rules:
            warnings.append(f"{step_type} step has no validation rules")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            recommendations=[]
        )
    
    def _validate_config(self, config: PipelineConfig) -> List[str]:
        """Validate pipeline configuration."""
        errors = []
        
        if not config.schema:
            errors.append("Schema name cannot be empty")
        
        if config.min_bronze_rate < 0 or config.min_bronze_rate > 100:
            errors.append(f"min_bronze_rate must be between 0 and 100, got {config.min_bronze_rate}")
        
        if config.min_silver_rate < 0 or config.min_silver_rate > 100:
            errors.append(f"min_silver_rate must be between 0 and 100, got {config.min_silver_rate}")
        
        if config.min_gold_rate < 0 or config.min_gold_rate > 100:
            errors.append(f"min_gold_rate must be between 0 and 100, got {config.min_gold_rate}")
        
        if config.max_parallel_workers < 1:
            errors.append(f"max_parallel_workers must be at least 1, got {config.max_parallel_workers}")
        
        return errors
    
    def _validate_bronze_steps(self, bronze_steps: Dict[str, BronzeStep]) -> tuple[List[str], List[str]]:
        """Validate bronze steps."""
        errors = []
        warnings = []
        
        if not bronze_steps:
            warnings.append("No bronze steps defined")
            return errors, warnings
        
        for name, step in bronze_steps.items():
            if not step.name:
                errors.append(f"Bronze step {name} must have a name")
            
            if not step.rules:
                warnings.append(f"Bronze step {name} has no validation rules")
        
        return errors, warnings
    
    def _validate_silver_steps(
        self, 
        silver_steps: Dict[str, SilverStep], 
        bronze_steps: Dict[str, BronzeStep]
    ) -> tuple[List[str], List[str]]:
        """Validate silver steps."""
        errors = []
        warnings = []
        
        for name, step in silver_steps.items():
            if not step.name:
                errors.append(f"Silver step {name} must have a name")
            
            if not step.rules:
                warnings.append(f"Silver step {name} has no validation rules")
            
            if hasattr(step, 'source_bronze') and step.source_bronze:
                if step.source_bronze not in bronze_steps:
                    errors.append(f"Silver step {name} references non-existent bronze step: {step.source_bronze}")
        
        return errors, warnings
    
    def _validate_gold_steps(
        self, 
        gold_steps: Dict[str, GoldStep], 
        silver_steps: Dict[str, SilverStep]
    ) -> tuple[List[str], List[str]]:
        """Validate gold steps."""
        errors = []
        warnings = []
        
        for name, step in gold_steps.items():
            if not step.name:
                errors.append(f"Gold step {name} must have a name")
            
            if not step.rules:
                warnings.append(f"Gold step {name} has no validation rules")
            
            if hasattr(step, 'source_silvers') and step.source_silvers:
                for silver_name in step.source_silvers:
                    if silver_name not in silver_steps:
                        errors.append(f"Gold step {name} references non-existent silver step: {silver_name}")
        
        return errors, warnings
    
    def _validate_dependencies(
        self,
        bronze_steps: Dict[str, BronzeStep],
        silver_steps: Dict[str, SilverStep],
        gold_steps: Dict[str, GoldStep]
    ) -> tuple[List[str], List[str]]:
        """Validate step dependencies."""
        errors = []
        warnings = []
        
        # Check for circular dependencies
        # This is a simplified check - in practice, you'd use a proper graph algorithm
        all_steps = {**bronze_steps, **silver_steps, **gold_steps}
        
        # Check for missing dependencies
        for step_name, step in all_steps.items():
            if hasattr(step, 'depends_on') and step.depends_on:
                for dep in step.depends_on:
                    if dep not in all_steps:
                        errors.append(f"Step {step_name} depends on non-existent step: {dep}")
        
        return errors, warnings
    
    def _generate_recommendations(
        self,
        config: PipelineConfig,
        bronze_steps: Dict[str, BronzeStep],
        silver_steps: Dict[str, SilverStep],
        gold_steps: Dict[str, GoldStep]
    ) -> List[str]:
        """Generate optimization recommendations."""
        recommendations = []
        
        # Performance recommendations
        if len(silver_steps) > 5 and not config.enable_parallel_silver:
            recommendations.append("Consider enabling parallel silver execution for better performance")
        
        if config.max_parallel_workers < 4 and len(silver_steps) > 3:
            recommendations.append("Consider increasing max_parallel_workers for better parallelization")
        
        # Data quality recommendations
        if config.min_bronze_rate < 90:
            recommendations.append("Consider increasing bronze data quality threshold")
        
        if config.min_silver_rate < 95:
            recommendations.append("Consider increasing silver data quality threshold")
        
        # Architecture recommendations
        if len(bronze_steps) == 0:
            recommendations.append("Consider adding bronze steps for data validation")
        
        if len(gold_steps) == 0:
            recommendations.append("Consider adding gold steps for business analytics")
        
        return recommendations
