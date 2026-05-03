import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
os.environ["APP_PROFILE"] = "local"

from src.config import load_settings

def main():
    settings = load_settings()
    print("MDM Settings loaded:")
    print("Has district_mapping:", bool(settings.mdm.district_mapping), len(settings.mdm.district_mapping))
    print("Has property_type_mapping:", bool(settings.mdm.property_type_mapping), len(settings.mdm.property_type_mapping))
    print("property_type_mapping content:", settings.mdm.property_type_mapping)

if __name__ == "__main__":
    main()
