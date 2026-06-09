import sys
from unittest.mock import MagicMock

# Try importing real pyspark before setting up mocks
try:
    import pyspark
    sys.has_real_pyspark = True
except ImportError:
    sys.has_real_pyspark = False

if not sys.has_real_pyspark:
    # Define placeholder classes for type checking / imports
    class MockDataFrame:
        pass

    class MockRow:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
        def __getitem__(self, item):
            return getattr(self, item)

    pyspark_mock = MagicMock()
    pyspark_sql_mock = MagicMock()
    pyspark_sql_mock.DataFrame = MockDataFrame
    pyspark_sql_mock.Row = MockRow
    
    # Inject mocks into sys.modules
    sys.modules['pyspark'] = pyspark_mock
    sys.modules['pyspark.sql'] = pyspark_sql_mock
    sys.modules['pyspark.sql.functions'] = MagicMock()
    sys.modules['pyspark.sql.types'] = MagicMock()
    sys.modules['pyspark.sql.window'] = MagicMock()
    sys.modules['delta'] = MagicMock()
    sys.modules['delta.tables'] = MagicMock()
    sys.modules['pandas'] = MagicMock()
