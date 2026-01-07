import sys
from pathlib import Path


def project_root() -> Path:
    """
    Resolves the project root directory.

    Works for:
    - Source execution
    - PyInstaller frozen exe
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parents[2]
    return Path(__file__).resolve().parents[2]


BASE_DIR = project_root()
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "nse_equity_universe.db"
DB_PATH_SAMPLE = DATA_DIR / "nse_equity_universe_sample.db"
