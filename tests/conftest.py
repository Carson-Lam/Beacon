import sys
from pathlib import Path

# Mirrors /opt/spark-apps in the containers so CI/CD tests import exactly what the jobs import
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "spark"))