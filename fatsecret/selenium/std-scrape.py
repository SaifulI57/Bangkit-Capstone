from functionality import *
from typing import Any
import time


def start() -> None:
    """
    Starting Scrape.
    
    Call: 
        initWebDriver()
        getProductBrand(Chrome, str)
    Sleep: 
        nextAction(int)
    
    Return: 
        None
    """
    listProduct = getListBrand()
    driver: Chrome = initWebDriver()
    # try:
    #     last = getLastScrape()
    #     listProduct = listProduct[listProduct.index(last["brand"]):]
    # except:
    #     pass
    # getProductBrand(driver, "ABC")
    for i in listProduct:
        getProductBrand(driver, i)
        time.sleep(nextAction(2))
    
    
    driver.quit()

def main() -> None:
    """_summary_
    
    Call:
        Start()
    
    Return:
        None
    """
    start()
    
if __name__ == "__main__":
    main()    
