from abc import ABC, abstractmethod
from typing import Any, Dict
from utility.logger import get_logger

logger = get_logger()
class BaseValidator(ABC):
    def __init__(self, validation_rules: Dict[str, Any]):
        self.validation_rules = validation_rules

    @abstractmethod
    def validate(self, data: Dict[str, Any]) -> bool:
        """Validate data against rules"""
        pass

    def validate_number(self, field: str, value: Any) -> bool:
        """Validate numeric fields"""
        rules = self.validation_rules.get(field, {})
        if rules.get('type') != 'number':
            return True

        try:
            num_value = float(value)
            if 'min' in rules and num_value < rules['min']:
                logger.error(f"Value {num_value} below minimum {rules['min']} for {field}")
                return False
            if 'max' in rules and num_value > rules['max']:
                logger.error(f"Value {num_value} above maximum {rules['max']} for {field}")
                return False
            return True
        except ValueError:
            logger.error(f"Invalid number value for {field}: {value}")
            return False

    def validate_string(self, field: str, value: Any) -> bool:
        """Validate string fields"""
        rules = self.validation_rules.get(field, {})
        if rules.get('type') != 'string':
            return True

        if not isinstance(value, str):
            logger.error(f"Invalid string value for {field}: {value}")
            return False

        if 'min_length' in rules and len(value) < rules['min_length']:
            logger.error(f"String too short for {field}")
            return False
        if 'max_length' in rules and len(value) > rules['max_length']:
            logger.error(f"String too long for {field}")
            return False

        return True 