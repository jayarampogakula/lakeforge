"""
LakeForge Datatype Check Validator
"""
from pyspark.sql import DataFrame
from typing import Dict, Any

class DatatypeValidator:
    """
    Validator to check that a column has the expected datatype in a DataFrame.
    """
    
    @staticmethod
    def validate(df: DataFrame, column: str, expected_type: str) -> Dict[str, Any]:
        """
        Validate if the datatype of a column matches expected_type.
        
        Args:
            df: DataFrame to validate
            column: Name of column to check
            expected_type: Expected Spark datatype (e.g. 'int', 'string', 'decimal', 'double')
            
        Returns:
            Validation result dictionary
        """
        # Get schema fields
        fields = {f.name: f for f in df.schema.fields}
        if column not in fields:
            return {
                "rule_type": "datatype_check",
                "column": column,
                "expected_type": expected_type,
                "actual_type": None,
                "passed": False,
                "error": f"Column '{column}' does not exist in schema",
                "failed_records": df.count()
            }
            
        actual_type = fields[column].dataType
        actual_type_str = str(actual_type).lower()
        expected_type_lower = expected_type.lower()
        
        # Normalization map
        type_map = {
            "int": "integer",
            "integer": "integer",
            "long": "long",
            "string": "string",
            "double": "double",
            "float": "float",
            "date": "date",
            "timestamp": "timestamp",
            "boolean": "boolean"
        }
        
        norm_expected = type_map.get(expected_type_lower, expected_type_lower)
        
        passed = False
        if "decimal" in norm_expected and "decimal" in actual_type_str:
            passed = True
        elif norm_expected in actual_type_str:
            passed = True
            
        return {
            "rule_type": "datatype_check",
            "column": column,
            "expected_type": expected_type,
            "actual_type": str(actual_type),
            "passed": passed,
            "failed_records": 0 if passed else df.count()
        }
