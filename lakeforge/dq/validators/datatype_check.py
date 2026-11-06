"""
LakeForge Datatype Check Validator
"""
try:
    from pyspark.sql import DataFrame
except ImportError:
    DataFrame = object
from typing import Dict, Any

class DatatypeValidator:
    """
    Validator to check that a column has the expected datatype in a DataFrame.
    Supports SQL and Spark type family matching (e.g. 'int' matches IntegerType and LongType/BigInt).
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
        
        # Family mapping for cross-platform SQL/Spark type compatibility
        type_families = {
            "integer": {"int", "integer", "long", "bigint", "short", "smallint", "byte", "tinyint"},
            "float": {"double", "float", "real"},
            "decimal": {"decimal", "numeric"},
            "string": {"string", "varchar", "char", "text"},
            "timestamp": {"timestamp", "datetime", "timestampntz"},
            "date": {"date"},
            "boolean": {"boolean", "bool"}
        }

        # Resolve family of expected type
        expected_family = None
        for fam, aliases in type_families.items():
            if expected_type_lower in aliases:
                expected_family = fam
                break
        if not expected_family:
            expected_family = expected_type_lower

        # Resolve family of actual type
        actual_family = None
        for fam, aliases in type_families.items():
            if any(alias in actual_type_str for alias in aliases):
                actual_family = fam
                break
        if not actual_family:
            actual_family = actual_type_str

        passed = False
        if "decimal" in expected_type_lower and "decimal" in actual_type_str:
            passed = True
        elif expected_family == actual_family:
            passed = True
        elif expected_type_lower in actual_type_str:
            passed = True
            
        return {
            "rule_type": "datatype_check",
            "column": column,
            "expected_type": expected_type,
            "actual_type": str(actual_type),
            "passed": passed,
            "failed_records": 0 if passed else df.count()
        }
