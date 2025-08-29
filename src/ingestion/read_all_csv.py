from src.utils.logger import logger
from time import time
import threading


if __name__ == "__main__":
    from src.ingestion.df_parsing.vacs01 import main as vacs01_main
    from src.ingestion.df_parsing.vacs02 import main as vacs02_main
    from src.ingestion.df_parsing.vacs03 import main as vacs03_main
    from src.ingestion.df_parsing.x06 import main as x06_main

    start_time = time()

    threads = [
        threading.Thread(target=vacs01_main),
        threading.Thread(target=vacs02_main),
        threading.Thread(target=vacs03_main),
        threading.Thread(target=x06_main)
        ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    #vacs01_main()
    #vacs02_main()
    #vacs03_main()
    #x06_main()
    end_time = time()
    logger.info(f"All scripts completed in {end_time - start_time:.2f} seconds.")