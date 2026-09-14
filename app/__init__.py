"""MOP Agent application package.

Load local environment configuration before submodules freeze path-based
settings at import time. Container deployments already inject the same values,
so this keeps CLI, tests, and uvicorn entry points consistent with production.
"""

from dotenv import load_dotenv

load_dotenv()
