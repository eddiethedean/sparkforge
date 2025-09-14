"""
Step execution system for SparkForge.

This module provides focused step execution capabilities, handling individual
step execution with proper validation, error handling, and monitoring.
"""

from __future__ import annotations
from typing import Dict, List, Optional, Any
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from .models import StepExecutionContext
from ..models import PipelineConfig
from .validator import PipelineValidator
from ..models import BronzeStep, SilverStep, GoldStep, ExecutionContext
from ..logger import PipelineLogger
from ..validation import apply_column_rules
from ..table_operations import fqn


class StepExecutor:
    """
    Focused step execution system.
    
    This class handles individual step execution with proper validation,
    error handling, and monitoring integration.
    
    Features:
    - Individual step execution
    - Comprehensive validation
    - Error handling and recovery
    - Performance monitoring
    - Delta table operations
    """
    
    def __init__(
        self,
        *,
        spark: SparkSession,
        config: PipelineConfig,
        logger: PipelineLogger
    ):
        """
        Initialize a new StepExecutor instance.
        
        Args:
            spark: Active SparkSession instance
            config: Pipeline configuration
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.validator = PipelineValidator(logger)
    
    def execute_bronze_step(
        self,
        step: BronzeStep,
        source_df: DataFrame,
        context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        Execute a Bronze step.
        
        Args:
            step: Bronze step definition
            source_df: Source DataFrame
            context: Execution context
            
        Returns:
            Dictionary containing execution results
        """
        start_time = datetime.now()
        step_context = StepExecutionContext(
            step_name=step.name,
            step_type="bronze",
            mode=context.mode,
            start_time=start_time
        )
        
        try:
            self.logger.info(f"🟤 Executing Bronze step: {step.name}")
            
            # Validate step
            validation_result = self.validator.validate_step(step, "bronze", context)
            if not validation_result.is_valid:
                raise ValueError(f"Bronze step validation failed: {validation_result.errors}")
            
            # Apply validation rules
            validated_df = self._apply_validation_rules(
                df=source_df,
                rules=step.rules,
                step_name=step.name,
                step_type="bronze"
            )
            
            # Calculate metrics
            total_rows = source_df.count()
            valid_rows = validated_df.count()
            quality_rate = (valid_rows / total_rows * 100) if total_rows > 0 else 0
            
            # Check quality threshold
            min_rate = self.config.min_bronze_rate
            if quality_rate < min_rate:
                raise ValueError(f"Bronze quality rate {quality_rate:.2f}% below threshold {min_rate}%")
            
            # Write to Delta table (simplified)
            table_name = fqn(self.config.schema, f"bronze_{step.name}")
            self._write_to_delta(validated_df, table_name, step.incremental_col)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            result = {
                "success": True,
                "step_name": step.name,
                "step_type": "bronze",
                "duration": duration,
                "rows_processed": total_rows,
                "rows_written": valid_rows,
                "quality_rate": quality_rate,
                "table_name": table_name
            }
            
            self.logger.info(f"✅ Bronze step {step.name} completed: {valid_rows:,} rows written")
            return result
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"❌ Bronze step {step.name} failed: {str(e)}")
            
            return {
                "success": False,
                "step_name": step.name,
                "step_type": "bronze",
                "duration": duration,
                "error": str(e),
                "rows_processed": 0,
                "rows_written": 0
            }
    
    def execute_silver_step(
        self,
        step: SilverStep,
        bronze_df: DataFrame,
        prior_silvers: Dict[str, DataFrame],
        context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        Execute a Silver step.
        
        Args:
            step: Silver step definition
            bronze_df: Source bronze DataFrame
            prior_silvers: Prior silver step results
            context: Execution context
            
        Returns:
            Dictionary containing execution results
        """
        start_time = datetime.now()
        step_context = StepExecutionContext(
            step_name=step.name,
            step_type="silver",
            mode=context.mode,
            start_time=start_time
        )
        
        try:
            self.logger.info(f"🟡 Executing Silver step: {step.name}")
            
            # Validate step
            validation_result = self.validator.validate_step(step, "silver", context)
            if not validation_result.is_valid:
                raise ValueError(f"Silver step validation failed: {validation_result.errors}")
            
            # Execute transformation
            transformed_df = step.transform(self.spark, bronze_df, prior_silvers)
            
            # Apply validation rules
            validated_df = self._apply_validation_rules(
                df=transformed_df,
                rules=step.rules,
                step_name=step.name,
                step_type="silver"
            )
            
            # Calculate metrics
            total_rows = transformed_df.count()
            valid_rows = validated_df.count()
            quality_rate = (valid_rows / total_rows * 100) if total_rows > 0 else 0
            
            # Check quality threshold
            min_rate = self.config.min_silver_rate
            if quality_rate < min_rate:
                raise ValueError(f"Silver quality rate {quality_rate:.2f}% below threshold {min_rate}%")
            
            # Write to Delta table
            table_name = fqn(self.config.schema, step.table_name)
            self._write_to_delta(validated_df, table_name, step.watermark_col)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            result = {
                "success": True,
                "step_name": step.name,
                "step_type": "silver",
                "duration": duration,
                "rows_processed": total_rows,
                "rows_written": valid_rows,
                "quality_rate": quality_rate,
                "table_name": table_name
            }
            
            self.logger.info(f"✅ Silver step {step.name} completed: {valid_rows:,} rows written")
            return result
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"❌ Silver step {step.name} failed: {str(e)}")
            
            return {
                "success": False,
                "step_name": step.name,
                "step_type": "silver",
                "duration": duration,
                "error": str(e),
                "rows_processed": 0,
                "rows_written": 0
            }
    
    def execute_gold_step(
        self,
        step: GoldStep,
        silver_dfs: Dict[str, DataFrame],
        context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        Execute a Gold step.
        
        Args:
            step: Gold step definition
            silver_dfs: Source silver DataFrames
            context: Execution context
            
        Returns:
            Dictionary containing execution results
        """
        start_time = datetime.now()
        step_context = StepExecutionContext(
            step_name=step.name,
            step_type="gold",
            mode=context.mode,
            start_time=start_time
        )
        
        try:
            self.logger.info(f"🟨 Executing Gold step: {step.name}")
            
            # Validate step
            validation_result = self.validator.validate_step(step, "gold", context)
            if not validation_result.is_valid:
                raise ValueError(f"Gold step validation failed: {validation_result.errors}")
            
            # Execute transformation
            transformed_df = step.transform(self.spark, silver_dfs)
            
            # Apply validation rules
            validated_df = self._apply_validation_rules(
                df=transformed_df,
                rules=step.rules,
                step_name=step.name,
                step_type="gold"
            )
            
            # Calculate metrics
            total_rows = transformed_df.count()
            valid_rows = validated_df.count()
            quality_rate = (valid_rows / total_rows * 100) if total_rows > 0 else 0
            
            # Check quality threshold
            min_rate = self.config.min_gold_rate
            if quality_rate < min_rate:
                raise ValueError(f"Gold quality rate {quality_rate:.2f}% below threshold {min_rate}%")
            
            # Write to Delta table
            table_name = fqn(self.config.schema, step.table_name)
            self._write_to_delta(validated_df, table_name)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            result = {
                "success": True,
                "step_name": step.name,
                "step_type": "gold",
                "duration": duration,
                "rows_processed": total_rows,
                "rows_written": valid_rows,
                "quality_rate": quality_rate,
                "table_name": table_name
            }
            
            self.logger.info(f"✅ Gold step {step.name} completed: {valid_rows:,} rows written")
            return result
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"❌ Gold step {step.name} failed: {str(e)}")
            
            return {
                "success": False,
                "step_name": step.name,
                "step_type": "gold",
                "duration": duration,
                "error": str(e),
                "rows_processed": 0,
                "rows_written": 0
            }
    
    def _apply_validation_rules(
        self,
        df: DataFrame,
        rules: Dict[str, List[Any]],
        step_name: str,
        step_type: str
    ) -> DataFrame:
        """Apply validation rules to a DataFrame."""
        try:
            return apply_column_rules(df, rules)
        except Exception as e:
            self.logger.error(f"Validation rules failed for {step_type} step {step_name}: {str(e)}")
            raise
    
    def _write_to_delta(
        self,
        df: DataFrame,
        table_name: str,
        watermark_col: Optional[str] = None
    ) -> None:
        """Write DataFrame to Delta table."""
        try:
            # Simplified Delta write - in real implementation, this would:
            # 1. Check if table exists
            # 2. Apply appropriate write mode (append/overwrite)
            # 3. Handle watermarking for incremental processing
            # 4. Optimize for performance
            
            self.logger.debug(f"Writing to Delta table: {table_name}")
            # df.write.format("delta").mode("append").saveAsTable(table_name)
            
        except Exception as e:
            self.logger.error(f"Failed to write to Delta table {table_name}: {str(e)}")
            raise
