from scripts import extract, transform, load
from scripts.common.utils import logger

if __name__ == "__main__":
    logger.info("Starting extraction process..")
    extract.main()
    logger.info("Starting transformation process..")
    transform.main()
    logger.info("Starting loading process..")
    load.main()
