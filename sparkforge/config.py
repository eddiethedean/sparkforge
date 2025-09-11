# config.py
"""
Enhanced configuration management utilities for the Pipeline Builder.

This module provides comprehensive configuration management with:
- Type-safe configuration creation
- Validation and error handling
- Predefined configuration templates
- Environment-specific configurations
- Configuration serialization/deserialization
- Configuration validation and migration
- Environment variable support
- Configuration inheritance and merging

Key Features:
- Immutable configuration objects
- Comprehensive validation
- Multiple configuration templates
- Environment-aware defaults
- JSON/YAML serialization support
- Configuration versioning
- Hot-reloading capabilities
- Configuration diffing and comparison
"""

from __future__ import annotations
from typing import Any, Dict, Optional, Union, List, Tuple, Set
from dataclasses import asdict, field, dataclass
from enum import Enum
import json
import os
import yaml
from pathlib import Path
import hashlib
from datetime import datetime
import logging

from .models import (
    PipelineConfig, 
    ValidationThresholds, 
    ParallelConfig,
    PipelinePhase,
    ExecutionMode
)


# ============================================================================
# Custom Exceptions
# ============================================================================

class ConfigurationError(Exception):
    """Base exception for configuration-related errors."""
    pass


class ConfigurationValidationError(ConfigurationError):
    """Raised when configuration validation fails."""
    pass


class ConfigurationTemplateError(ConfigurationError):
    """Raised when configuration template operations fail."""
    pass


class ConfigurationSerializationError(ConfigurationError):
    """Raised when configuration serialization/deserialization fails."""
    pass


# ============================================================================
# Configuration Templates
# ============================================================================

class ConfigTemplate(Enum):
    """Predefined configuration templates for different environments and use cases."""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    HIGH_PERFORMANCE = "high_performance"
    CONSERVATIVE = "conservative"
    DEBUG = "debug"
    MINIMAL = "minimal"
    CUSTOM = "custom"


class ConfigEnvironment(Enum):
    """Environment types for configuration management."""
    LOCAL = "local"
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"


# ============================================================================
# Configuration Metadata
# ============================================================================

@dataclass
class ConfigMetadata:
    """Metadata for configuration objects."""
    version: str = "1.0.0"
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = "system"
    environment: str = "development"
    template: str = "default"
    description: str = ""
    tags: Set[str] = field(default_factory=set)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary."""
        return {
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "created_by": self.created_by,
            "environment": self.environment,
            "template": self.template,
            "description": self.description,
            "tags": list(self.tags)
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ConfigMetadata:
        """Create metadata from dictionary."""
        return cls(
            version=data.get("version", "1.0.0"),
            created_at=datetime.fromisoformat(data.get("created_at", datetime.utcnow().isoformat())),
            updated_at=datetime.fromisoformat(data.get("updated_at", datetime.utcnow().isoformat())),
            created_by=data.get("created_by", "system"),
            environment=data.get("environment", "development"),
            template=data.get("template", "default"),
            description=data.get("description", ""),
            tags=set(data.get("tags", []))
        )


# ============================================================================
# Enhanced Configuration Manager
# ============================================================================

class ConfigManager:
    """
    Enhanced configuration manager with comprehensive features.
    
    Features:
    - Type-safe configuration creation
    - Predefined templates for different environments
    - Validation and error handling
    - Serialization/deserialization support
    - Environment-aware defaults
    - Configuration merging and inheritance
    - Configuration versioning
    - Hot-reloading capabilities
    - Configuration diffing and comparison
    """
    
    # Predefined configuration templates
    TEMPLATES = {
        ConfigTemplate.DEVELOPMENT: {
            "thresholds": {"bronze": 80.0, "silver": 85.0, "gold": 90.0},
            "parallel": {"enabled": True, "max_workers": 2, "timeout_secs": 60},
            "verbose": True,
            "environment": "development"
        },
        ConfigTemplate.TESTING: {
            "thresholds": {"bronze": 70.0, "silver": 75.0, "gold": 80.0},
            "parallel": {"enabled": False, "max_workers": 1, "timeout_secs": 30},
            "verbose": True,
            "environment": "testing"
        },
        ConfigTemplate.STAGING: {
            "thresholds": {"bronze": 90.0, "silver": 95.0, "gold": 98.0},
            "parallel": {"enabled": True, "max_workers": 4, "timeout_secs": 300},
            "verbose": False,
            "environment": "staging"
        },
        ConfigTemplate.PRODUCTION: {
            "thresholds": {"bronze": 99.0, "silver": 99.5, "gold": 99.9},
            "parallel": {"enabled": True, "max_workers": 8, "timeout_secs": 600},
            "verbose": False,
            "environment": "production"
        },
        ConfigTemplate.HIGH_PERFORMANCE: {
            "thresholds": {"bronze": 95.0, "silver": 98.0, "gold": 99.0},
            "parallel": {"enabled": True, "max_workers": 16, "timeout_secs": 1200},
            "verbose": False,
            "environment": "production"
        },
        ConfigTemplate.CONSERVATIVE: {
            "thresholds": {"bronze": 99.5, "silver": 99.8, "gold": 99.9},
            "parallel": {"enabled": False, "max_workers": 1, "timeout_secs": 1800},
            "verbose": True,
            "environment": "production"
        },
        ConfigTemplate.DEBUG: {
            "thresholds": {"bronze": 50.0, "silver": 60.0, "gold": 70.0},
            "parallel": {"enabled": False, "max_workers": 1, "timeout_secs": 3600},
            "verbose": True,
            "environment": "development"
        },
        ConfigTemplate.MINIMAL: {
            "thresholds": {"bronze": 60.0, "silver": 70.0, "gold": 80.0},
            "parallel": {"enabled": False, "max_workers": 1, "timeout_secs": 60},
            "verbose": False,
            "environment": "testing"
        }
    }
    
    # Environment variable mappings
    ENV_MAPPINGS = {
        "PIPELINE_SCHEMA": "schema",
        "PIPELINE_BRONZE_THRESHOLD": ("thresholds", "bronze"),
        "PIPELINE_SILVER_THRESHOLD": ("thresholds", "silver"),
        "PIPELINE_GOLD_THRESHOLD": ("thresholds", "gold"),
        "PIPELINE_PARALLEL_ENABLED": ("parallel", "enabled"),
        "PIPELINE_MAX_WORKERS": ("parallel", "max_workers"),
        "PIPELINE_TIMEOUT_SECS": ("parallel", "timeout_secs"),
        "PIPELINE_VERBOSE": "verbose"
    }
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize configuration manager."""
        self.logger = logger or logging.getLogger(__name__)
        self._config_cache: Dict[str, PipelineConfig] = {}
        self._metadata_cache: Dict[str, ConfigMetadata] = {}
    
    # ========================================================================
    # Configuration Creation Methods
    # ========================================================================
    
    @classmethod
    def create_config(
        cls,
        schema: str,
        min_bronze_rate: float = 95.0,
        min_silver_rate: float = 98.0,
        min_gold_rate: float = 99.0,
        enable_parallel_silver: bool = True,
        max_parallel_workers: int = 4,
        timeout_secs: int = 300,
        verbose: bool = True,
        metadata: Optional[ConfigMetadata] = None
    ) -> PipelineConfig:
        """
        Create a validated pipeline configuration with custom parameters.
        
        Args:
            schema: Database schema name
            min_bronze_rate: Bronze layer validation threshold (0-100)
            min_silver_rate: Silver layer validation threshold (0-100)
            min_gold_rate: Gold layer validation threshold (0-100)
            enable_parallel_silver: Whether to enable parallel silver execution
            max_parallel_workers: Maximum number of parallel workers
            timeout_secs: Timeout for parallel operations in seconds
            verbose: Whether to enable verbose logging
            metadata: Optional configuration metadata
            
        Returns:
            Validated PipelineConfig object
            
        Raises:
            ConfigurationValidationError: If any parameter is invalid
        """
        try:
            thresholds = ValidationThresholds(
                bronze=min_bronze_rate,
                silver=min_silver_rate,
                gold=min_gold_rate
            )
            
            parallel = ParallelConfig(
                enabled=enable_parallel_silver,
                max_workers=max_parallel_workers,
                timeout_secs=timeout_secs
            )
            
            config = PipelineConfig(
                schema=schema,
                thresholds=thresholds,
                parallel=parallel,
                verbose=verbose
            )
            
            # Validate configuration
            cls.validate_config(config)
            
            return config
            
        except Exception as e:
            raise ConfigurationValidationError(f"Failed to create configuration: {e}")
    
    @classmethod
    def from_template(
        cls,
        schema: str,
        template: ConfigTemplate,
        **overrides
    ) -> PipelineConfig:
        """
        Create configuration from a predefined template.
        
        Args:
            schema: Database schema name
            template: Predefined configuration template
            **overrides: Optional parameter overrides
            
        Returns:
            PipelineConfig object based on template
            
        Raises:
            ConfigurationTemplateError: If template is invalid
        """
        if template not in cls.TEMPLATES:
            raise ConfigurationTemplateError(f"Unknown template: {template}")
        
        try:
            template_config = cls.TEMPLATES[template].copy()
            
            # Apply overrides
            template_config.update(overrides)
            
            # Extract template values
            thresholds_config = template_config.get("thresholds", {})
            parallel_config = template_config.get("parallel", {})
            
            thresholds = ValidationThresholds(
                bronze=thresholds_config.get("bronze", 95.0),
                silver=thresholds_config.get("silver", 98.0),
                gold=thresholds_config.get("gold", 99.0)
            )
            
            parallel = ParallelConfig(
                enabled=parallel_config.get("enabled", True),
                max_workers=parallel_config.get("max_workers", 4),
                timeout_secs=parallel_config.get("timeout_secs", 300)
            )
            
            config = PipelineConfig(
                schema=schema,
                thresholds=thresholds,
                parallel=parallel,
                verbose=template_config.get("verbose", True)
            )
            
            # Validate configuration
            cls.validate_config(config)
            
            return config
            
        except Exception as e:
            raise ConfigurationTemplateError(f"Failed to create configuration from template {template}: {e}")
    
    @classmethod
    def from_environment(
        cls,
        schema: Optional[str] = None,
        template: Optional[ConfigTemplate] = None,
        **overrides
    ) -> PipelineConfig:
        """
        Create configuration from environment variables.
        
        Args:
            schema: Database schema name (defaults to PIPELINE_SCHEMA env var)
            template: Base template to use (optional)
            **overrides: Additional parameter overrides
            
        Returns:
            PipelineConfig object based on environment variables
        """
        # Get schema from environment if not provided
        if schema is None:
            schema = os.getenv("PIPELINE_SCHEMA")
            if not schema:
                raise ConfigurationError("Schema must be provided or set in PIPELINE_SCHEMA environment variable")
        
        # Start with template if provided
        if template:
            config = cls.from_template(schema, template, **overrides)
        else:
            config = cls.create_config(schema, **overrides)
        
        # Apply environment variable overrides
        env_overrides = cls._extract_env_overrides()
        if env_overrides:
            config = cls.merge_configs(config, env_overrides)
        
        return config
    
    @classmethod
    def from_file(
        cls,
        file_path: Union[str, Path],
        format: str = "auto"
    ) -> Tuple[PipelineConfig, ConfigMetadata]:
        """
        Load configuration from file.
        
        Args:
            file_path: Path to configuration file
            format: File format ("json", "yaml", or "auto")
            
        Returns:
            Tuple of (PipelineConfig, ConfigMetadata)
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise ConfigurationError(f"Configuration file not found: {file_path}")
        
        # Auto-detect format if not specified
        if format == "auto":
            if file_path.suffix.lower() in [".json"]:
                format = "json"
            elif file_path.suffix.lower() in [".yaml", ".yml"]:
                format = "yaml"
            else:
                raise ConfigurationError(f"Unable to determine file format for: {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                if format == "json":
                    data = json.load(f)
                elif format == "yaml":
                    data = yaml.safe_load(f)
                else:
                    raise ConfigurationError(f"Unsupported format: {format}")
            
            # Extract metadata if present
            metadata_data = data.pop("metadata", {})
            metadata = ConfigMetadata.from_dict(metadata_data)
            
            # Create configuration
            config = cls.from_dict(data)
            
            return config, metadata
            
        except Exception as e:
            raise ConfigurationSerializationError(f"Failed to load configuration from {file_path}: {e}")
    
    # ========================================================================
    # Configuration Serialization Methods
    # ========================================================================
    
    @staticmethod
    def from_dict(config_dict: Dict[str, Any]) -> PipelineConfig:
        """Create configuration from dictionary."""
        schema = config_dict.get("schema")
        if not schema:
            raise ConfigurationValidationError("schema is required")
        
        thresholds_config = config_dict.get("thresholds", {})
        thresholds = ValidationThresholds(
            bronze=thresholds_config.get("bronze", 95.0),
            silver=thresholds_config.get("silver", 98.0),
            gold=thresholds_config.get("gold", 99.0)
        )
        
        parallel_config = config_dict.get("parallel", {})
        parallel = ParallelConfig(
            enabled=parallel_config.get("enabled", True),
            max_workers=parallel_config.get("max_workers", 4),
            timeout_secs=parallel_config.get("timeout_secs", 300)
        )
        
        return PipelineConfig(
            schema=schema,
            thresholds=thresholds,
            parallel=parallel,
            verbose=config_dict.get("verbose", True)
        )
    
    @staticmethod
    def to_dict(config: PipelineConfig) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "schema": config.schema,
            "thresholds": asdict(config.thresholds),
            "parallel": asdict(config.parallel),
            "verbose": config.verbose
        }
    
    @staticmethod
    def to_json(config: PipelineConfig, metadata: Optional[ConfigMetadata] = None, indent: int = 2) -> str:
        """Convert configuration to JSON string."""
        config_dict = ConfigManager.to_dict(config)
        if metadata:
            config_dict["metadata"] = metadata.to_dict()
        return json.dumps(config_dict, indent=indent, default=str)
    
    @staticmethod
    def from_json(json_str: str) -> Tuple[PipelineConfig, Optional[ConfigMetadata]]:
        """Create configuration from JSON string."""
        try:
            data = json.loads(json_str)
            
            # Extract metadata if present
            metadata_data = data.pop("metadata", None)
            metadata = ConfigMetadata.from_dict(metadata_data) if metadata_data else None
            
            config = ConfigManager.from_dict(data)
            return config, metadata
            
        except Exception as e:
            raise ConfigurationSerializationError(f"Failed to parse JSON configuration: {e}")
    
    @staticmethod
    def to_yaml(config: PipelineConfig, metadata: Optional[ConfigMetadata] = None) -> str:
        """Convert configuration to YAML string."""
        config_dict = ConfigManager.to_dict(config)
        if metadata:
            config_dict["metadata"] = metadata.to_dict()
        return yaml.dump(config_dict, default_flow_style=False, sort_keys=False)
    
    @staticmethod
    def from_yaml(yaml_str: str) -> Tuple[PipelineConfig, Optional[ConfigMetadata]]:
        """Create configuration from YAML string."""
        try:
            data = yaml.safe_load(yaml_str)
            
            # Extract metadata if present
            metadata_data = data.pop("metadata", None)
            metadata = ConfigMetadata.from_dict(metadata_data) if metadata_data else None
            
            config = ConfigManager.from_dict(data)
            return config, metadata
            
        except Exception as e:
            raise ConfigurationSerializationError(f"Failed to parse YAML configuration: {e}")
    
    # ========================================================================
    # Configuration Validation Methods
    # ========================================================================
    
    @staticmethod
    def validate_config(config: PipelineConfig) -> None:
        """Validate a configuration object."""
        if not config.schema or not isinstance(config.schema, str):
            raise ConfigurationValidationError("schema must be a non-empty string")
        
        if not 0 <= config.thresholds.bronze <= 100:
            raise ConfigurationValidationError("bronze threshold must be between 0 and 100")
        
        if not 0 <= config.thresholds.silver <= 100:
            raise ConfigurationValidationError("silver threshold must be between 0 and 100")
        
        if not 0 <= config.thresholds.gold <= 100:
            raise ConfigurationValidationError("gold threshold must be between 0 and 100")
        
        if config.parallel.max_workers < 1:
            raise ConfigurationValidationError("max_workers must be at least 1")
        
        if config.parallel.max_workers > 32:
            raise ConfigurationValidationError("max_workers should not exceed 32")
        
        if config.parallel.timeout_secs < 1:
            raise ConfigurationValidationError("timeout_secs must be at least 1")
    
    @staticmethod
    def validate_template(template: ConfigTemplate) -> bool:
        """Validate a configuration template."""
        if template not in ConfigManager.TEMPLATES:
            return False
        
        template_config = ConfigManager.TEMPLATES[template]
        
        # Check required fields
        required_fields = ["thresholds", "parallel", "verbose"]
        for field in required_fields:
            if field not in template_config:
                return False
        
        # Validate thresholds
        thresholds = template_config["thresholds"]
        for phase in ["bronze", "silver", "gold"]:
            if phase not in thresholds:
                return False
            if not 0 <= thresholds[phase] <= 100:
                return False
        
        # Validate parallel config
        parallel = template_config["parallel"]
        if "enabled" not in parallel or "max_workers" not in parallel:
            return False
        if not 1 <= parallel["max_workers"] <= 32:
            return False
        
        return True
    
    # ========================================================================
    # Configuration Comparison and Diffing
    # ========================================================================
    
    @staticmethod
    def compare_configs(config1: PipelineConfig, config2: PipelineConfig) -> Dict[str, Any]:
        """Compare two configurations and return differences."""
        dict1 = ConfigManager.to_dict(config1)
        dict2 = ConfigManager.to_dict(config2)
        
        differences = {}
        
        # Compare each field
        for key in dict1:
            if key not in dict2:
                differences[key] = {"old": dict1[key], "new": None}
            elif dict1[key] != dict2[key]:
                differences[key] = {"old": dict1[key], "new": dict2[key]}
        
        # Check for new fields in config2
        for key in dict2:
            if key not in dict1:
                differences[key] = {"old": None, "new": dict2[key]}
        
        return differences
    
    @staticmethod
    def config_hash(config: PipelineConfig) -> str:
        """Generate a hash for configuration comparison."""
        config_dict = ConfigManager.to_dict(config)
        config_str = json.dumps(config_dict, sort_keys=True)
        return hashlib.md5(config_str.encode()).hexdigest()
    
    # ========================================================================
    # Configuration Merging and Inheritance
    # ========================================================================
    
    @staticmethod
    def merge_configs(base_config: PipelineConfig, overrides: Dict[str, Any]) -> PipelineConfig:
        """Merge configuration overrides into base configuration."""
        base_dict = ConfigManager.to_dict(base_config)
        
        # Deep merge overrides
        merged_dict = ConfigManager._deep_merge(base_dict, overrides)
        
        return ConfigManager.from_dict(merged_dict)
    
    @staticmethod
    def _deep_merge(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = base.copy()
        
        for key, value in overrides.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = ConfigManager._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    @classmethod
    def _extract_env_overrides(cls) -> Dict[str, Any]:
        """Extract configuration overrides from environment variables."""
        overrides = {}
        
        for env_var, config_path in cls.ENV_MAPPINGS.items():
            value = os.getenv(env_var)
            if value is not None:
                # Convert value to appropriate type
                if isinstance(config_path, tuple):
                    # Nested path
                    if config_path[0] not in overrides:
                        overrides[config_path[0]] = {}
                    overrides[config_path[0]][config_path[1]] = cls._convert_env_value(value)
                else:
                    # Direct path
                    overrides[config_path] = cls._convert_env_value(value)
        
        return overrides
    
    @staticmethod
    def _convert_env_value(value: str) -> Any:
        """Convert environment variable string to appropriate type."""
        # Boolean values
        if value.lower() in ["true", "false"]:
            return value.lower() == "true"
        
        # Numeric values
        try:
            if "." in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            pass
        
        # String value
        return value
    
    # ========================================================================
    # Configuration Caching and Management
    # ========================================================================
    
    def cache_config(self, key: str, config: PipelineConfig, metadata: Optional[ConfigMetadata] = None):
        """Cache a configuration."""
        self._config_cache[key] = config
        if metadata:
            self._metadata_cache[key] = metadata
    
    def get_cached_config(self, key: str) -> Optional[PipelineConfig]:
        """Get cached configuration."""
        return self._config_cache.get(key)
    
    def clear_cache(self):
        """Clear configuration cache."""
        self._config_cache.clear()
        self._metadata_cache.clear()
    
    def list_cached_configs(self) -> List[str]:
        """List all cached configuration keys."""
        return list(self._config_cache.keys())


# ============================================================================
# Factory Functions
# ============================================================================

def get_default_config(schema: str) -> PipelineConfig:
    """Get default configuration for a schema."""
    return ConfigManager.create_config(schema=schema)


def get_high_performance_config(schema: str) -> PipelineConfig:
    """Get high-performance configuration with more parallel workers."""
    return ConfigManager.create_config(
        schema=schema,
        enable_parallel_silver=True,
        max_parallel_workers=8,
        verbose=False
    )


def get_conservative_config(schema: str) -> PipelineConfig:
    """Get conservative configuration with higher validation thresholds."""
    return ConfigManager.create_config(
        schema=schema,
        min_bronze_rate=99.0,
        min_silver_rate=99.5,
        min_gold_rate=99.9,
        enable_parallel_silver=False,
        verbose=True
    )


def get_template_config(schema: str, template: ConfigTemplate, **overrides) -> PipelineConfig:
    """Get configuration from template."""
    return ConfigManager.from_template(schema, template, **overrides)


def get_environment_config(schema: Optional[str] = None, template: Optional[ConfigTemplate] = None) -> PipelineConfig:
    """Get configuration from environment variables."""
    return ConfigManager.from_environment(schema, template)


def load_config_from_file(file_path: Union[str, Path], format: str = "auto") -> Tuple[PipelineConfig, ConfigMetadata]:
    """Load configuration from file."""
    return ConfigManager.from_file(file_path, format)


def save_config_to_file(
    config: PipelineConfig, 
    file_path: Union[str, Path], 
    metadata: Optional[ConfigMetadata] = None,
    format: str = "auto"
) -> None:
    """Save configuration to file."""
    file_path = Path(file_path)
    
    # Auto-detect format if not specified
    if format == "auto":
        if file_path.suffix.lower() in [".json"]:
            format = "json"
        elif file_path.suffix.lower() in [".yaml", ".yml"]:
            format = "yaml"
        else:
            raise ConfigurationError(f"Unable to determine file format for: {file_path}")
    
    try:
        if format == "json":
            content = ConfigManager.to_json(config, metadata)
        elif format == "yaml":
            content = ConfigManager.to_yaml(config, metadata)
        else:
            raise ConfigurationError(f"Unsupported format: {format}")
        
        with open(file_path, 'w') as f:
            f.write(content)
            
    except Exception as e:
        raise ConfigurationSerializationError(f"Failed to save configuration to {file_path}: {e}")


# ============================================================================
# Configuration Utilities
# ============================================================================

def validate_config_file(file_path: Union[str, Path]) -> bool:
    """Validate a configuration file."""
    try:
        ConfigManager.from_file(file_path)
        return True
    except Exception:
        return False


def diff_config_files(file1: Union[str, Path], file2: Union[str, Path]) -> Dict[str, Any]:
    """Compare two configuration files."""
    config1, _ = ConfigManager.from_file(file1)
    config2, _ = ConfigManager.from_file(file2)
    return ConfigManager.compare_configs(config1, config2)


def migrate_config(config: PipelineConfig, from_version: str, to_version: str) -> PipelineConfig:
    """Migrate configuration between versions."""
    # This is a placeholder for future configuration migration logic
    # For now, just return the config as-is
    return config