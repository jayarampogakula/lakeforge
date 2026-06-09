"""
LakeForge Dynamic Runner Utility
Dynamically loads and executes Python modules and functions.
"""
import importlib
import sys
from typing import Any

class DynamicRunner:
    """
    Utility class to load python modules and execute functions at runtime.
    """
    
    @staticmethod
    def load_and_execute(module_path: str, function_name: str, *args, **kwargs) -> Any:
        """
        Dynamically load a python module and execute a function from it.
        
        Args:
            module_path: Dot-separated path to module (e.g. 'lakeforge.transformations.customer')
            function_name: Name of function or class to call (e.g. 'clean_customer_name')
            *args: Positional arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            The return value of the executed function
        """
        try:
            # Make sure module path is in python path
            if "/Workspace/Users/jayarampogakula@gmail.com/lakeforge" not in sys.path:
                sys.path.append("/Workspace/Users/jayarampogakula@gmail.com/lakeforge")
                
            # Import the module
            module = importlib.import_module(module_path)
            
            # Fetch the function
            func = getattr(module, function_name)
            
            # Execute
            return func(*args, **kwargs)
        except Exception as e:
            raise ImportError(f"Failed to dynamically load and execute {module_path}.{function_name}: {str(e)}")
