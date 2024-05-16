import ast
from random import randint
import pandas as pd
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ChromeOptions
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.remote.webelement import WebElement
from typing import Any
from bs4 import BeautifulSoup
import time
import json
from loguru import logger
import sys
# from dotenv import dotenv_values
# import redis

# global var
index = 0
dn = False
statei = True

def getLastScrape() -> pd.Series:
    """
    Retrieves the last scraped data from the fatsecret.csv file.

    Returns:
        pd.Series: The last row of data from the fatsecret.csv file, or None if the file doesn't exist.
    """
    try:
        lastDf = pd.read_csv("./fatsecret.csv")
        lastDf = lastDf.iloc[-1]
    except:
        lastDf = None
    
    return lastDf  

try:
    last = getLastScrape()
except:
    last = ""
# redis_client: redis = redis.from_url(dotenv_values(".env").get("REDIS_URL"))
logger.add(sys.stdout, colorize=True, format="<green>{time}</green> <level>{message}</level>")
logger.add("./log/logger-selenium.log")

def initWebDriver() -> Chrome:
    """
    Sets up the WebDriver for Chrome with custom options to prevent Selenium detection.

    Returns:
        Chrome: An instance of the Chrome WebDriver with a user profile and custom options.
    """
    
    
    # path: str = "D:\\Coding\\bangkit\\capstone\\dataset\\scrape\\fatsecret\\selenium\\chromedriver.exe"
    userProfile: str = r"C:\Users\saifu\AppData\Local\Google\Chrome\User Data"
    # service: Service = Service(path)
    options: ChromeOptions  = ChromeOptions()
    # options.binary_location = path
    
    
    # flags to prevent selenium detection
    options.add_argument(f"user-data-dir={userProfile}")
    options.add_argument("profile-directory=Profile 7")
    options.add_argument("remote-debugging-pipe")
    options.add_argument("disable-extensions")
    options.add_argument("disable-infobars")
    options.add_argument("disable-blink-features=AutomationControlled")

    # create instance of Chrome
    driver: Chrome = Chrome(options=options)
    driver.set_page_load_timeout(7)
    # change property value of the navigator for webdriver to undifined
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    return driver

def nextAction(i: int=6) -> int:
    """
    Generates a random number between the given range to delay actions and prevent Selenium detection.

    Args:
        i (int, optional): The starting value of the range. Defaults to 6.

    Returns:
        int: A random integer between i and 10.

    Raises:
        ValueError: If i is greater than 10.
    """
    if i > 10:
        raise ValueError("Greater than 10 must be lower than 10")
    
    
    return randint(i,10)




def ids(i: int = 1) -> str:
    """
    Generates a unique ID string.

    Args:
        i (int, optional): A value to be appended to the ID string. Defaults to 1.

    Returns:
        str: A unique ID string in the format "current_timestamp-i".
    """
    return str(int(time.time())) + f"-{i}"

def convert_to_csv(frame: pd.DataFrame) -> None:
    """
    Converts a pandas DataFrame to a CSV file or merges it with an existing CSV file.

    Args:
        frame (pd.DataFrame): The DataFrame to be converted or merged.
    """
    logger.info("Checking available Data")
    try:
        ex = pd.read_csv("./fatsecret.csv", index_col="scrape_order")
        logger.info("Data is available, Starting merge...")
        concated = pd.concat([ex, frame], ignore_index=False)
    except:
        logger.info("Data is not available, Converting to csv...")
        concated = frame
        # concated.set_index("scrape_order")
    
    concated.to_csv("./fatsecret.csv", index="scrape_order")
    logger.info("Done converting to csv ✅")
    



def getProductBrand(webdriver: Chrome, brand: str) -> None:
    """
    Scrapes product information from the fatsecret.co.id website for a given brand.

    Args:
        webdriver (Chrome): An instance of the Chrome WebDriver.
        brand (str): The brand name to search for.
    """
    
    # webdriver.get("https://google.com/")
    # time.sleep(1000)
    # with open("./list_all_brand.txt") as f:
    #     brands = ast.literal_eval(f.read())
    #     len_brand = len(brands)
    webdriver.get(f"https://www.fatsecret.co.id/kalori-gizi/search?q={brand}")
    try:
        cookie_close_button = WebDriverWait(webdriver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "cc-compliance")))
        cookie_close_button.click()  # Klik untuk menutup pop-up cookie
        logger.info("Cookie accepted")
        
    except:
        logger.info("Cookie already accepted")
    
    try:
        dts = get_dataset_list()
    except:
        dts = None
    def untilProductN(driver: Chrome) -> None:
        
        WebDriverWait(webdriver, 10).until(EC.presence_of_element_located((By.CLASS_NAME,"generic")))
        # time.sleep(nextAction(3))
        
        listE = driver.find_elements(By.CLASS_NAME, "prominent")
        listB = driver.find_elements(By.CLASS_NAME, "brand")
        listB = [("".join(x.text.replace("(","").replace(")",""))) for x in listB]
        list_dpi: dict[str, Any] = {
            "brand": brand,
            "list_product": []
        }
        global index, dn, last, statei
        
        if last is not None and statei:
            index = int(last["scrape_order"][last["scrape_order"].index("-")+1:])
            statei = False
        
        # try:
        #     if last is not None:
        #         listE_filtered = [x.get_attribute("href") for x in listE]
        #         found = listE_filtered.index(last["product_link"])
        #         if found:
        #             dn = True
        #             listE = listE[found+1:]
        #             index = int(last["scrape_order"][-3:])
        #             if len(listE[found+1:]) == 1:
        #                 pass
        # except:
        #     if dn:
        #         pass
        #     else:
                # return
        
        for i, x in enumerate(listE):
            # index += 1
            link_product = x.get_attribute("href")
            list_dpi["list_product"].append(x.text)
            WebDriverWait(webdriver, 20).until(EC.element_to_be_clickable((By.CLASS_NAME,"prominent")))
            try:
                comp = skip_if_exist(dts, {"brand": listB[i], "product_link": link_product})
                if comp:
                    logger.info("Skipping, product exists in dataset")
                    return
                else:
                    index += 1
                    logger.info("Adding product to dataset...")
                    pass
            except:
                pass
            
            x.click()
            # time.sleep(nextAction(9))
            sources = parseBs4(webdriver.page_source)
            sources = parseN(sources, {"scrape_order": ids(index), "search_link": f"https://www.fatsecret.co.id/kalori-gizi/search?q={brand}", "product_link": link_product})
            generateListProduct(listB[i], list_dpi["list_product"])
            # setCompletedProduct(brand, list_dpi["list_product"][0]["nama_product"])
            convert_to_csv(sources)
            webdriver.back()
            time.sleep(2)
            logger.info(f"Done scrape product {x.text} ✅")
        
        # stateListProduct = False
        # sLP = {}
        
        
        # try:
        #     with open("list_products.json", "r") as f:
        #         if f.read() == "":
        #             stateListProduct = False
        #         else:
        #             stateListProduct = True
        #             sLP = json.loads(f.read())
        #         f.close()
                
        #     with open("list_products.json", "w") as f:
        #         if(stateListProduct == True):
        #             f.write(json.dumps({**sLP, **list_dpi}))
        #             f.close()
        #         else:
        #             f.write(json.dumps(list_dpi))
        #             f.close()
        # except:
        #     logger.error("Something went wrong while reading list_products.json or write")
        # list[0].click()
        # print(webdriver.page_source)
        
    try:
        ele = webdriver.find_element(By.CLASS_NAME, "next")
        while ele:
            untilProductN(webdriver)
            # time.sleep(10)
            WebDriverWait(webdriver, 20).until(EC.element_to_be_clickable((By.CLASS_NAME,"next"))).click()
            ele = webdriver.find_element(By.CLASS_NAME, "next")
            # time.sleep(nextAction(5))
    except:
        untilProductN(webdriver)
        logger.info("End Pagination")
        
    # generateLogBrand(brand)
    time.sleep(nextAction(2))
    # with open("./log/brand.log","r") as f:
    #     try:
    #         brands = ast.literal_eval(f.read())
    #     except:
    #         brands = []
    #     f.close()
    # with open("./log/brand.log","w") as f:
    #     if brand in brands:
    #         pass
    #     else:
    #         f.write(str(brands.append(brand)))
    #     f.close()
        # webdriver.back()
        
        
        # raise NotImplementedError("untilProductN not implemented")

def get_dataset_list() -> pd.DataFrame:
    """
    Retrieves the existing dataset from the fatsecret.csv file.

    Returns:
        pd.DataFrame: The existing dataset, or None if the file doesn't exist.
    """
    try:
        csv = pd.read_csv("./fatsecret.csv")
    except:
        csv = None
        
    return csv

def skip_if_exist(dt: pd.DataFrame, comp: dict) -> bool:
    """
    Checks if a product already exists in the dataset.

    Args:
        dt (pd.DataFrame): The existing dataset.
        comp (dict): A dictionary containing the brand and product link to check.

    Returns:
        bool: True if the product already exists in the dataset, False otherwise.
    """
    if not dt[dt["product_link"].str.contains(comp.get("product_link"), regex=False)].empty:
        return True
    else:
        return False

# @logger.catch
# def setCompletedProduct(brand: str, product: str) -> None:
#     try:
#         res: redis = redis_client.get(f"{brand}").decode("utf-8")
#         dec = ast.literal_eval(res)
#         redis.set(f"{brand}", str({**dec, f"{product}": {"status":"completed"}}))
        
#     except:
#         redis_client.set(f"{brand}", str({f"{product}": {"status":"completed"}}))
        
#     try:
#         with open(f"{product}.json", "r") as f:
#             result = json.loads(f.read())
#             f.close()
#         with open(f"{product}.json", "w") as f:
#             f.write(json.dumps({**result, f"{product}": {"status":"completed"}}))
#             f.close()
#     except:
#         logger.info("Failed to read content")
#         with open(f"{product}.json", "w") as f:
#             f.write(json.dumps({f"{brand}": {f"{product}": {"status":"completed"}}}))
#             f.close()
    
    
#     try:
#         compl: redis = redis_client.get("completed").decode("utf-8")    
#         data = ast.literal_eval(compl)
#         data[f"{brand}"] = {**data[f"{brand}"], f"{product}": {"status":"completed"}}
#         redis_client.set("completed", str(data))
#     except:
#         redis_client.set("completed", str({f"{brand}": {f"{product}": {"status":"completed"}}}))
#         logger.error("Failed to set completed key")
        
        
# @logger.catch
# def completedBrand() -> str:
#     logger.info("Checking where the scrape ends...")
#     logger.info("Starting check locally...")
#     try:
#         with open("./log/brand.log","r") as f:
#             brand = ast.literal_eval(f.read())
#             f.close()
#     except:
#         with open("./log/brand.log","w") as f:
#             brand = []
#             f.close()
#     if brand == "" or brand == None or brand == []:
#         logger.info("No log brand founded in local")
    
    
    
#     completed: redis = redis_client.get("completed").decode("utf-8")    
#     if completed == None:
#         logger.info("There is no completed data")
#     else:
#         return ast.literal_eval(completed)
#     return {"product": 0, "brand": 0}
    
#     raise NotImplementedError("completedBrand not implemented")

def generateListProduct(brand: str, product: list[str]) -> None:
    """
    Generates a list of products for a given brand and stores it in a JSON file.

    Args:
        brand (str): The brand name.
        product (list[str]): A list of product names.
    """
    logger.info("Generating product List...")
    try:
        with open("./list/list_products.json", "r") as f:
            try:
                loads = json.loads(f.read())
            except:
                loads = {}
            f.close()
    except:
        loads = {}
        logger.info("File Not Found")
        
        
    with open("./list/list_products.json", "w") as f:
        
        loads[("".join([x for x in brand if x.isalpha()]).lower())] = {"list_product": product}
        try:
            f.write(json.dumps(loads))
            f.close()
        except:
            logger.error("Failed to write list")
            f.close()           
    logger.info("Done ✅")

def generateLogBrand(brand: str) -> None:
    """
    Generates a log of scraped brands and stores it in a file.

    Args:
        brand (str): The brand name to log.
    """
    logger.info("Generating log brand...")
    with open("./log/brand.log", "r") as f:
        res = f.read()
        if res == "":
            result = [brand]
        else:
            result = ast.literal_eval(res)
            result.append(brand)
        f.close()
    
    with open("./log/brand.log", "w") as f:
        f.write(str(result))
        f.close()
    logger.info("Done ✅")
    



def getListBrand() -> list:
    """
    Retrieves a list of all brands from a file.

    Returns:
        list: A list of brand names.
    """


    with open("./list_all_brand.txt", "r") as f:
        res: list = ast.literal_eval(f.read())    
    
    return res
    
def parseN(soup: BeautifulSoup, args: dict[str, Any]) -> pd.DataFrame:
    """
    Parses the nutrition information from a BeautifulSoup object and returns a pandas DataFrame.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object containing the HTML content.
        args (dict[str, Any]): A dictionary containing additional arguments.

    Returns:
        pd.DataFrame: A DataFrame containing the parsed nutrition information.
    """
    logger.info("Parsing Nutrition information...")
    
    
    nutrient_dict: dict[str, Any] = {
        "scrape_order": "",
        "search_link": "",
        "product_link": "",
        "brand": "",
        "product_name": "",
        "ukuran_porsi": "",
        "energi": "",
        "lemak": "",
        "lemak_jenuh": "",
        "lemak_trans": "",
        "kolestrol": "",
        "protein": "",
        "karbohirdat": "",
        "serat": "",
        "gula": "",
        "sodium": "",
        "kalium": "",
        "natrium": ""
    }
    
    # find content nutrient
    s: BeautifulSoup = soup.select("div.nutrition_facts")[0]
    p: BeautifulSoup = soup.select("div.summarypanelcontent")[0].find("h2",{"class":"manufacturer"})
    
    product: dict[str, str] = {
        "brand": p.text,
        "product_name": p.findNextSiblings()[0].text
    }
    
    porsi: dict[str, str] = {
        s.find("div", {"class":"serving_size_label"}).text.lower().replace(" ","_"): s.find("div",{"class":"serving_size_value"}).text,
    }

    # sorting
    nutrient: list = [x.text.replace("","energi")if x.text == "" else x.text.lower().replace(" ","_") for x in s.find_all("div", {"class":["nutrient","black","left"]})[4:]][2:]
    nutrient: dict = {nutrient[x]:nutrient[x+1] for x in range(0, len(nutrient),2)}
    nutrient: dict = {**args, **product, **porsi, **nutrient}
    # print(nutrient)
    # refactoring, so it can be converted to dataframe
    nutrient_dict = {key: [nutrient.get(key, 0)] for key in nutrient_dict.keys()}
    
    # convert to dataframe
    result: pd.DataFrame = pd.DataFrame(nutrient_dict)
    result = result.set_index("scrape_order")
    
    logger.info("Done parsing Nutrient to dataframe ✅")
    
    return result

def parseBs4(html: str) -> BeautifulSoup:
    """_summary_

    Args:
        html (str): _description_

    Returns:
        BeautifulSoup: _description_
    """
    return BeautifulSoup(html, "html.parser")







