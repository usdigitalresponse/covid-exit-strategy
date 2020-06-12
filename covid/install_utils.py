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
    print(f"Installing package {package}...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    print(f"Install of package {package} completed.")


def install_rpy2():
    print("Trying to install r-base...")
    subprocess.run(["apt", "install", "r-base"])
    # subprocess.run('conda install -c conda-forge r-base', shell=True)
    print("Finished installing r-base.")

    os.environ["RPY2_CFFI_MODE"] = "ABI"
    install_package("rpy2==3.3.2")
