from .pipeline import HoWDe_labelling
from importlib.metadata import version, PackageNotFoundError
import warnings

warnings.filterwarnings("ignore")

try:
    # Must match [project].name in pyproject.toml
    __version__ = version("HoWDe")
except PackageNotFoundError:
    # Fallback if HoWDe is not installed
    __version__ = "0.0.0"

