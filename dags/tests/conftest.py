import sys
from pathlib import Path

# dags/ is the import root in Composer (dfa_common is imported as a top-level package).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
