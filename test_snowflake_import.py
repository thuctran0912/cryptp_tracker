import sys
import os

print("Current working directory:", os.getcwd())
print("__file__:", __file__)

# Add the project root directory to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

print("Python path:")
for path in sys.path:
    print(path)

try:
    from src.snowflake_handler import SnowflakeHandler
    print("Successfully imported SnowflakeHandler")
    print(f"SnowflakeHandler is defined in: {SnowflakeHandler.__module__}")
except ImportError as e:
    print(f"Failed to import SnowflakeHandler: {e}")
    import traceback
    traceback.print_exc()