"""
LakeForge Data Quality Engine
Executes data quality rules and generates scorecards.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, lit, current_timestamp, sum as _sum
from typing import List, Dict, Any, Optional
from datetime import datetime


class DQEngine:
    """
    Data Quality engine for validating data against defined rules.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize DQ Engine.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        self.validation_results = []
    
    def run_null_check(
        self,
        df: DataFrame,
        column: str,
        threshold: float = 0.0
    ) -> Dict[str, Any]:
        """
        Check for null values in a column.
        
        Args:
            df: DataFrame to validate
            column: Column name to check
            threshold: Maximum allowed null percentage (0-1)
            
        Returns:
            Validation result dictionary
        """
        total_count = df.count()
        null_count = df.filter(col(column).isNull()).count()
        null_percentage = null_count / total_count if total_count > 0 else 0
        
        passed = null_percentage <= threshold
        
        return {
            "rule_type": "null_check",
            "column": column,
            "total_records": total_count,
            "null_count": null_count,
            "null_percentage": null_percentage,
            "threshold": threshold,
            "passed": passed,
            "failed_records": null_count if not passed else 0
        }
    
    def run_duplicate_check(
        self,
        df: DataFrame,
        columns: List[str],
        allow_duplicates: bool = False
    ) -> Dict[str, Any]:
        """
        Check for duplicate records based on specified columns.
        
        Args:
            df: DataFrame to validate
            columns: Columns to check for duplicates
            allow_duplicates: Whether duplicates are allowed
            
        Returns:
            Validation result dictionary
        """
        total_count = df.count()
        unique_count = df.select(columns).distinct().count()
        duplicate_count = total_count - unique_count
        
        passed = duplicate_count == 0 if not allow_duplicates else True
        
        return {
            "rule_type": "duplicate_check",
            "columns": columns,
            "total_records": total_count,
            "unique_records": unique_count,
            "duplicate_count": duplicate_count,
            "passed": passed,
            "failed_records": duplicate_count if not passed else 0
        }
    
    def run_regex_check(
        self,
        df: DataFrame,
        column: str,
        pattern: str,
        threshold: float = 1.0
    ) -> Dict[str, Any]:
        """
        Validate column values against regex pattern.
        
        Args:
            df: DataFrame to validate
            column: Column name to check
            pattern: Regex pattern to match
            threshold: Minimum match percentage required (0-1)
            
        Returns:
            Validation result dictionary
        """
        from pyspark.sql.functions import regexp_extract
        
        total_count = df.count()
        
        # Count matching records
        match_count = df.filter(
            regexp_extract(col(column).cast("string"), pattern, 0) != ""
        ).count()
        
        match_percentage = match_count / total_count if total_count > 0 else 0
        failed_count = total_count - match_count
        
        passed = match_percentage >= threshold
        
        return {
            "rule_type": "regex_check",
            "column": column,
            "pattern": pattern,
            "total_records": total_count,
            "matching_records": match_count,
            "match_percentage": match_percentage,
            "threshold": threshold,
            "passed": passed,
            "failed_records": failed_count if not passed else 0
        }
    
    def run_range_check(
        self,
        df: DataFrame,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        threshold: float = 1.0
    ) -> Dict[str, Any]:
        """
        Validate numeric column is within specified range.
        
        Args:
            df: DataFrame to validate
            column: Column name to check
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            threshold: Minimum in-range percentage required (0-1)
            
        Returns:
            Validation result dictionary
        """
        total_count = df.count()
        
        # Build range condition
        condition = col(column).isNotNull()
        if min_value is not None:
            condition = condition & (col(column) >= min_value)
        if max_value is not None:
            condition = condition & (col(column) <= max_value)
        
        in_range_count = df.filter(condition).count()
        in_range_percentage = in_range_count / total_count if total_count > 0 else 0
        failed_count = total_count - in_range_count
        
        passed = in_range_percentage >= threshold
        
        return {
            "rule_type": "range_check",
            "column": column,
            "min_value": min_value,
            "max_value": max_value,
            "total_records": total_count,
            "in_range_records": in_range_count,
            "in_range_percentage": in_range_percentage,
            "threshold": threshold,
            "passed": passed,
            "failed_records": failed_count if not passed else 0
        }
        
    def run_datatype_check(
        self,
        df: DataFrame,
        column: str,
        expected_type: str
    ) -> Dict[str, Any]:
        """
        Validate data type of a column.
        """
        from lakeforge.dq.validators.datatype_check import DatatypeValidator
        return DatatypeValidator.validate(df, column, expected_type)

    def run_allowed_values_check(
        self,
        df: DataFrame,
        column: str,
        allowed_values: List[Any]
    ) -> Dict[str, Any]:
        """
        Check if column values are within a list of allowed values.
        """
        total_count = df.count()
        invalid_count = df.filter(col(column).isNotNull() & (~col(column).isin(allowed_values))).count()
        passed = invalid_count == 0
        
        return {
            "rule_type": "allowed_values",
            "column": column,
            "allowed_values": allowed_values,
            "total_records": total_count,
            "failed_records": invalid_count,
            "passed": passed
        }

    def run_referential_integrity_check(
        self,
        df: DataFrame,
        source_column: str,
        reference_table: str,
        reference_column: str
    ) -> Dict[str, Any]:
        """
        Check referential integrity against a reference table.
        """
        total_count = df.count()
        try:
            ref_df = self.spark.table(reference_table)
            orphans_count = df.join(ref_df, df[source_column] == ref_df[reference_column], "left_anti").count()
            passed = orphans_count == 0
            message = "Referential integrity check passed" if passed else f"{orphans_count} orphaned keys found"
        except Exception as e:
            orphans_count = total_count
            passed = False
            message = f"Referential integrity check failed: {str(e)}"

        return {
            "rule_type": "referential_integrity",
            "source_column": source_column,
            "reference_table": reference_table,
            "reference_column": reference_column,
            "total_records": total_count,
            "failed_records": orphans_count,
            "passed": passed,
            "message": message
        }

    def run_row_count_threshold_check(
        self,
        df: DataFrame,
        baseline_count: int,
        threshold_percent: float
    ) -> Dict[str, Any]:
        """
        Check if total row count is within acceptable percentage deviation from baseline.
        """
        total_count = df.count()
        deviation = abs(total_count - baseline_count)
        allowed_deviation = baseline_count * threshold_percent
        passed = deviation <= allowed_deviation
        
        return {
            "rule_type": "row_count_threshold",
            "baseline_count": baseline_count,
            "threshold_percent": threshold_percent,
            "total_records": total_count,
            "failed_records": deviation if not passed else 0,
            "passed": passed
        }

    def run_null_rate_threshold_check(
        self,
        df: DataFrame,
        column: str,
        threshold: float
    ) -> Dict[str, Any]:
        """
        Check that null rate of a column does not exceed threshold.
        """
        total_count = df.count()
        null_count = df.filter(col(column).isNull()).count()
        null_rate = null_count / total_count if total_count > 0 else 0
        passed = null_rate <= threshold
        
        return {
            "rule_type": "null_rate_threshold",
            "column": column,
            "threshold": threshold,
            "actual_null_rate": null_rate,
            "total_records": total_count,
            "failed_records": null_count if not passed else 0,
            "passed": passed
        }
    
    def run_custom_sql_check(
        self,
        df: DataFrame,
        custom_sql: str,
        temp_view_name: str = "dq_check_view"
    ) -> Dict[str, Any]:
        """
        Run custom SQL validation query.
        Expected: SQL should return a single row with 'passed' boolean column.
        
        Args:
            df: DataFrame to validate
            custom_sql: SQL query to execute
            temp_view_name: Temporary view name for the DataFrame
            
        Returns:
            Validation result dictionary
        """
        # Create temp view
        df.createOrReplaceTempView(temp_view_name)
        
        try:
            # Execute custom SQL
            result_df = self.spark.sql(custom_sql)
            result = result_df.collect()[0]
            
            passed = result["passed"] if "passed" in result.asDict() else False
            
            return {
                "rule_type": "custom_sql",
                "custom_sql": custom_sql,
                "passed": passed,
                "result": result.asDict(),
                "failed_records": 0 if passed else df.count()
            }
        except Exception as e:
            return {
                "rule_type": "custom_sql",
                "custom_sql": custom_sql,
                "passed": False,
                "error": str(e),
                "failed_records": df.count()
            }
    
    def validate_dataframe(
        self,
        df: DataFrame,
        rules: List[Dict[str, Any]],
        quarantine_failures: bool = True
    ) -> Dict[str, Any]:
        """
        Validate DataFrame against multiple DQ rules.
        
        Args:
            df: DataFrame to validate
            rules: List of rule dictionaries
            quarantine_failures: Whether to create quarantine DataFrame
            
        Returns:
            Comprehensive validation results
        """
        results = {
            "validation_timestamp": datetime.now().isoformat(),
            "total_records": df.count(),
            "rules_executed": 0,
            "rules_passed": 0,
            "rules_failed": 0,
            "rule_results": [],
            "quarantine_df": None,
            "quarantine_count": 0
        }
        
        failed_dfs = []
        
        for rule in rules:
            # Support both monolithic pipeline_config and normal config keys
            raw_rule_type = rule.get("rule_type", rule.get("validation_type"))
            rule_name = rule.get("rule_name", raw_rule_type)
            
            # Map validation_type string to rule_type string
            val_map = {
                "not_null": "null_check",
                "unique": "duplicate_check",
                "regex": "regex_check",
                "range": "range_check",
                "custom_sql": "custom_sql",
                "datatype": "datatype_check"
            }
            rule_type = val_map.get(raw_rule_type, raw_rule_type)
            
            try:
                # Execute appropriate rule
                if rule_type == "null_check":
                    rule_result = self.run_null_check(
                        df,
                        rule["column"],
                        rule.get("threshold", 0.0)
                    )
                    if not rule_result["passed"] and rule.get("action", "quarantine") == "quarantine":
                        failed_dfs.append(df.filter(col(rule["column"]).isNull()))
                
                elif rule_type == "duplicate_check":
                    # Support singular column or list of columns
                    cols = rule.get("columns", [rule.get("column")] if rule.get("column") else [])
                    rule_result = self.run_duplicate_check(
                        df,
                        cols,
                        rule.get("allow_duplicates", False)
                    )
                    if not rule_result["passed"] and rule.get("action", "quarantine") == "quarantine":
                        dups = df.groupBy(cols).count().filter("count > 1").select(cols)
                        failed_dfs.append(df.join(dups, on=cols, how="inner"))
                
                elif rule_type == "regex_check":
                    rule_result = self.run_regex_check(
                        df,
                        rule["column"],
                        rule["pattern"],
                        rule.get("threshold", 1.0)
                    )
                    if not rule_result["passed"] and rule.get("action", "quarantine") == "quarantine":
                        from pyspark.sql.functions import regexp_extract
                        failed_dfs.append(df.filter(regexp_extract(col(rule["column"]).cast("string"), rule["pattern"], 0) == ""))
                
                elif rule_type == "range_check":
                    rule_result = self.run_range_check(
                        df,
                        rule["column"],
                        rule.get("min_value"),
                        rule.get("max_value"),
                        rule.get("threshold", 1.0)
                    )
                    if not rule_result["passed"] and rule.get("action", "quarantine") == "quarantine":
                        cond = col(rule["column"]).isNull()
                        if rule.get("min_value") is not None:
                            cond = cond | (col(rule["column"]) < rule["min_value"])
                        if rule.get("max_value") is not None:
                            cond = cond | (col(rule["column"]) > rule["max_value"])
                        failed_dfs.append(df.filter(cond))
                
                elif rule_type == "datatype_check":
                    rule_result = self.run_datatype_check(
                        df,
                        rule["column"],
                        rule.get("expected_type", rule.get("expected_type"))
                    )
                    if not rule_result["passed"] and rule.get("action", "quarantine") == "quarantine":
                        failed_dfs.append(df)
                        
                elif rule_type == "allowed_values":
                    rule_result = self.run_allowed_values_check(
                        df,
                        rule["column"],
                        rule["allowed_values"]
                    )
                    if not rule_result["passed"] and rule.get("action", "quarantine") == "quarantine":
                        failed_dfs.append(df.filter(col(rule["column"]).isNotNull() & (~col(rule["column"]).isin(rule["allowed_values"]))))
                        
                elif rule_type == "referential_integrity":
                    rule_result = self.run_referential_integrity_check(
                        df,
                        rule["source_column"],
                        rule["reference_table"],
                        rule["reference_column"]
                    )
                    if not rule_result["passed"] and rule.get("action", "quarantine") == "quarantine":
                        ref_df = self.spark.table(rule["reference_table"])
                        orphans = df.join(ref_df, df[rule["source_column"]] == ref_df[rule["reference_column"]], "left_anti")
                        failed_dfs.append(orphans)
                        
                elif rule_type == "row_count_threshold":
                    rule_result = self.run_row_count_threshold_check(
                        df,
                        rule["baseline_count"],
                        rule["threshold_percent"]
                    )
                    # For row_count_threshold, we alert or fail, cannot quarantine individual rows
                    
                elif rule_type == "null_rate_threshold":
                    rule_result = self.run_null_rate_threshold_check(
                        df,
                        rule["column"],
                        rule["threshold"]
                    )
                    if not rule_result["passed"] and rule.get("action", "quarantine") == "quarantine":
                        failed_dfs.append(df.filter(col(rule["column"]).isNull()))
                
                elif rule_type == "custom_sql":
                    rule_result = self.run_custom_sql_check(
                        df,
                        rule["custom_sql"]
                    )
                    if not rule_result["passed"] and rule.get("action", "quarantine") == "quarantine":
                        failed_dfs.append(df)
                
                else:
                    rule_result = {
                        "rule_type": rule_type,
                        "passed": False,
                        "error": f"Unknown rule type: {rule_type}",
                        "failed_records": df.count()
                    }
                
                rule_result["rule_name"] = rule_name
                rule_result["severity"] = rule.get("severity", "error")
                rule_result["action"] = rule.get("action", "quarantine")
                
                results["rules_executed"] += 1
                if rule_result["passed"]:
                    results["rules_passed"] += 1
                else:
                    results["rules_failed"] += 1
                
                # Align the key names for verification compatibility in notebooks
                # Add pass_rate, rule_name etc. if they are missing
                if "pass_rate" not in rule_result:
                    tot = rule_result.get("total_records", results["total_records"])
                    failed_cnt = rule_result.get("failed_records", 0)
                    rule_result["pass_rate"] = (tot - failed_cnt) / tot if tot > 0 else 0.0
                    rule_result["fail_count"] = failed_cnt
                
                results["rule_results"].append(rule_result)
                
            except Exception as e:
                results["rule_results"].append({
                    "rule_name": rule_name,
                    "rule_type": rule_type,
                    "passed": False,
                    "error": str(e),
                    "pass_rate": 0.0,
                    "fail_count": df.count()
                })
                results["rules_executed"] += 1
                results["rules_failed"] += 1
        
        # Create quarantine DataFrame if requested
        if quarantine_failures and failed_dfs:
            final_failed = failed_dfs[0]
            for f_df in failed_dfs[1:]:
                # Union and drop duplicates
                final_failed = final_failed.union(f_df)
            
            final_failed = final_failed.dropDuplicates()
            results["quarantine_df"] = final_failed
            results["quarantine_count"] = final_failed.count()
        else:
            # Return empty dataframe with same schema
            results["quarantine_df"] = df.limit(0)
            results["quarantine_count"] = 0
            
        return results

    def validate(
        self,
        df: DataFrame,
        rules: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Legacy/Simple validation interface matching existing notebook calls.
        Returns a list of rule execution summaries.
        """
        res = self.validate_dataframe(df, rules, quarantine_failures=False)
        return res["rule_results"]
    
    def write_quarantine_table(
        self,
        quarantine_df: DataFrame,
        catalog: str,
        schema: str,
        table_name: str,
        validation_results: Dict[str, Any]
    ):
        """
        Write quarantined records to Delta table.
        
        Args:
            quarantine_df: DataFrame with failed records
            catalog: Catalog name
            schema: Schema name
            table_name: Quarantine table name
            validation_results: Results from validation
        """
        # Add quarantine metadata
        quarantine_df = quarantine_df.withColumn("_quarantine_timestamp", current_timestamp())
        quarantine_df = quarantine_df.withColumn("_validation_timestamp", lit(validation_results["validation_timestamp"]))
        quarantine_df = quarantine_df.withColumn("_rules_failed", lit(validation_results["rules_failed"]))
        
        # Write to Delta table
        full_table_name = f"{catalog}.{schema}.{table_name}"
        quarantine_df.write.format("delta").mode("append").saveAsTable(full_table_name)
    
    def generate_scorecard(
        self,
        validation_results: Dict[str, Any],
        catalog: str,
        schema: str,
        table_name: str,
        scorecard_table: str = "dq_scorecard"
    ):
        """
        Generate DQ scorecard and save to table.
        
        Args:
            validation_results: Results from validation
            catalog: Catalog name
            schema: Schema name
            table_name: Source table name
            scorecard_table: Scorecard table name
        """
        # Calculate overall score
        total_rules = validation_results["rules_executed"]
        passed_rules = validation_results["rules_passed"]
        score = (passed_rules / total_rules * 100) if total_rules > 0 else 0
        
        scorecard_data = [{
            "table_name": f"{catalog}.{schema}.{table_name}",
            "validation_timestamp": validation_results["validation_timestamp"],
            "total_records": validation_results["total_records"],
            "rules_executed": total_rules,
            "rules_passed": passed_rules,
            "rules_failed": validation_results["rules_failed"],
            "dq_score": score,
            "quarantine_count": validation_results.get("quarantine_count", 0)
        }]
        
        scorecard_df = self.spark.createDataFrame(scorecard_data)
        
        # Write to scorecard table
        full_scorecard_table = f"{catalog}.{schema}.{scorecard_table}"
        scorecard_df.write.format("delta").mode("append").saveAsTable(full_scorecard_table)


def create_dq_engine(spark: Optional[SparkSession] = None) -> DQEngine:
    """
    Factory function to create DQ engine.
    
    Args:
        spark: SparkSession (creates new one if None)
        
    Returns:
        DQEngine instance
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-DQ-Engine").getOrCreate()
    
    return DQEngine(spark)
