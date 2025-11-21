from .pipeline import HoWDe_labelling
from importlib.metadata import version, PackageNotFoundError
import warnings

warnings.filterwarnings("ignore")

try:
    # This must match [project].name in pyproject.toml
    __version__ = version("HoWDe")
except PackageNotFoundError:
    # Fallback for cases where the package is not installed
    # (e.g. running directly from source without `pip install -e .`)
    __version__ = "0.0.0"

