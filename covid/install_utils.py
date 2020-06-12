import logging
import os
import subprocess
import sys

logger = logging.getLogger(__name__)


def install_package(package):
    """

    Args:
        package:

    Returns:

    """
    logger.debug(f"Installing package {package}...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    logger.debug(f"Install of package {package} completed.")


def install_rpy2():
    subprocess.run(["apt", "install", "r-base"])

    os.environ["RPY2_CFFI_MODE"] = "ABI"
    install_package("rpy2==3.3.2")
