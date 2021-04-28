import os
import coverage

print(f"\033[31menabling coverage: {os.environ.get('COVERAGE_PROCESS_START', 'unset')}\033[0m")

coverage.process_startup()
