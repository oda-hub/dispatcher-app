import os

print(f"\033[31menabling coverage: {os.environ.get('COVERAGE_PROCESS_START', 'unset')}\033[0m")

import coverage
coverage.process_startup()
