from abc import ABC, abstractmethod
from typing import Any, Dict
from utility.logger import get_logger

logger = get_logger()
class BaseTransformer(ABC):
    def __init__(self, transformation_rules: Dict[str, Any]):
        self.transformation_rules = transformation_rules

    @abstractmethod
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data according to rules"""
        pass

    def transform_number(self, field: str, value: Any) -> Any:
        """Transform numeric fields"""
        rules = self.transformation_rules.get(field, {})
        if rules.get('type') != 'float':
            return value

        try:
            return round(float(value), rules.get('decimal_places', 2))
        except ValueError:
            logger.warning(f"Could not transform {field} to float: {value}")
            return value

    def transform_string(self, field: str, value: Any) -> str:
        """Transform string fields"""
        rules = self.transformation_rules.get(field, {})
        if rules.get('type') != 'string':
            return str(value)

        transformed_value = str(value)
        if rules.get('trim', False):
            transformed_value = transformed_value.strip()
        if rules.get('lowercase', False):
            transformed_value = transformed_value.lower()

        return transformed_value 