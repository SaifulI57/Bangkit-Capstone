from typing import Any
import shutil
import pandas as pd
import httpx
import os
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ChromeOptions
from selenium.webdriver import Remote
from selenium import webdriver
from loguru import logger
import string
import asyncio
import re
import time
import json
import sys


logger.add(sys.stdout, colorize=True, format="<green>{time}</green> <level>{message}</level>")
logger.add("./log/logger-selenium.log")


def init_driver() -> webdriver:
    """
    Initialize and configure the Chrome WebDriver.
    
    Returns:
        webdriver: Configured Chrome WebDriver instance.
    """
    
    
    options: ChromeOptions = ChromeOptions()
    
    
    options.add_argument("remote-debugging-pipe")
    options.add_argument("disable-extensions")
    options.add_argument("disable-infobars")
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("disable-blink-features=AutomationControlled")
    options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    
    driver: webdriver = Remote(command_executor="http://localhost:4445", options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.set_page_load_timeout(30.0)


    return driver    

def get_header(driver: webdriver, brand: str) -> dict:
    """
    Retrieve the header information from the specified brand's page.
    
    Args:
        driver (webdriver): Selenium WebDriver instance.
        brand (str): Brand name to retrieve the header for.
    
    Returns:
        dict: Header information.
    """
    
    
    try:
        # Navigate to the target page
        driver.get(f'https://alfagift.id/b/{brand}')

        # Wait for the page to load
        time.sleep(5)

        header = {}
        
        # Retrieve performance logs
        logs = driver.get_log('performance')
        with open("./pepe.txt", "w") as f:
            f.write(str(logs))
            f.close()
        logs = [json.loads(log['message'])['message'] for log in logs]
        for log in logs:        
            try:
                if log['params']['headers']['fingerprint'] != "" and log['method'] == "Network.requestWillBeSentExtraInfo" and log['params']['headers'][':path'] != "/v2/categories":
                    header = log['params']['headers']
                    break            
            except:
                pass
    finally:
        # Close the WebDriver
        driver.quit()
        
        return header
    
    
async def get_data_brand(client: httpx.AsyncClient, id_brand: str, header: dict) -> dict:
    """
    Asynchronously retrieve brand data using HTTPX.
    
    Args:
        client (httpx.AsyncClient): HTTPX asynchronous client instance.
        id_brand (str): Brand ID to retrieve data for.
        header (dict): Request headers.
    
    Returns:
        dict: Brand data.
    """
    
    
    try:
        header[':path'] = re.sub(r'(?<=/brand/)\d+',id_brand, header[':path'])
        key = {"auth": header[":authority"], "path": header[":path"]}
        key_to_remove = [':authority', ':method', ':path', ':scheme']
        
        for k in key_to_remove:
            if k in header:
                del header[k]
    except:
        key = {"auth": "webcommerce-gw.alfagift.id", "path":f"/v2/products/brand/{id_brand}?storeId=&start=0&limit=60"}
    resp = await get_data(client, f"https://{key['auth']+key['path']}", header)

    return resp

def get_df(product: list) -> pd.DataFrame:
    """
    Convert product list to a pandas DataFrame.
    
    Args:
        product (list): List of product data.
    
    Returns:
        pd.DataFrame: DataFrame containing product information.
    """
    list_product = []
    for i in product:
        temp = {"id":"", "product":"", "brand":"", "meta_brand":"", "image":"", "category_1":"", "category_2":"", "category_3":"","category_4":""}
        
        for j in i.keys():
            if j == "products":
                for k in i[j]:
                    temp['id'] = k['productId']
                    temp['product'] = k['productName']
                    temp['brand'] = i['currentBrandName']
                    temp['meta_brand'] = i['brandMetaKeyword']
                    temp['image'] = k['image']
                    temp['category_1'] = k['categoryNameLvl0']
                    temp['category_2'] = k['categoryNameLvl1']
                    temp['category_3'] = k['categoryNameLvl2']
                    temp['category_4'] = k['categoryNameLvl3']
                    
                    list_product.append(temp.copy())
    return pd.DataFrame(list_product)


def sort_directory() -> None:
    """
    Sort files into directories based on categories read from a CSV file.
    """
    
    def make_directory(path):
        try:
            os.makedirs(path, exist_ok=True)
            print(f"Directory '{path}' created successfully")
        except Exception as e:
            print(f"Failed to create directory '{path}': {e}")
    def move_file(source, destination):
        try:
            shutil.move(source, destination)
            print(f"File '{source}' moved to '{destination}' successfully")
        except Exception as e:
            print(f"Failed to move file '{source}' to '{destination}': {e}")

    res = pd.read_csv("./alfagift.csv")
    
    key_in = res['category_1'].unique()
    dict_in = {}
    
    for i in key_in:
        make_directory(f"./images/{i.lower()}")
        dict_in[i] = res[res['category_1'] == i]
        
        for n in dict_in[i]['product']:
                move_file(f"./images/{n.replace(' ', '_')}.jpg", f"./images/{i.lower()}/{n.replace(' ', '_')}.jpg")
        
    
    



async def get_images(client: httpx.AsyncClient, product: list) -> list:
    """
    Asynchronously download images for the given products.
    
    Args:
        client (httpx.AsyncClient): HTTPX asynchronous client instance.
        product (list): List of product data.
    
    Returns:
        list: List of product data with images downloaded.
    """
    list_product = []
    for i in product:
        temp = {"id":"", "product":"", "brand":"", "meta_brand":"", "image":"", "category_1":"", "category_2":"", "category_3":"","category_4":""}
        
        for j in i.keys():
            if j == "products":
                for k in i[j]:
                    temp['id'] = k['productId']
                    temp['product'] = k['productName']
                    temp['brand'] = i['currentBrandName']
                    temp['meta_brand'] = i['brandMetaKeyword']
                    temp['image'] = k['image']
                    temp['category_1'] = k['categoryNameLvl0']
                    temp['category_2'] = k['categoryNameLvl1']
                    temp['category_3'] = k['categoryNameLvl2']
                    temp['category_4'] = k['categoryNameLvl3']
                    
                    list_product.append(temp.copy())
                    
                    
    MAX_REQUEST = 50  
    REQUEST_COUNT = 0
    for i in list_product:
        if i['image'] == "" or i['image'] == None:
            pass
        else:
            res = await get_data_bytes(client, i['image'])
            REQUEST_COUNT += 1
            logger.info(f"Request ke {REQUEST_COUNT}")
            try:
                with open(f"./images/{i['product'].replace(' ', '_')}.jpg","wb") as f:
                    f.write(res)
                    f.close()
            except:
                logger.info("failed to write")
            if REQUEST_COUNT == MAX_REQUEST:
                asyncio.sleep(60)
                REQUEST_COUNT = 0
                
    return list_product
        
    
async def get_data_bytes(client: httpx.AsyncClient, url: str) -> Any:
    """
    Asynchronously retrieve data bytes from a URL.
    
    Args:
        client (httpx.AsyncClient): HTTPX asynchronous client instance.
        url (str): URL to retrieve data from.
    
    Returns:
        Any: Retrieved data bytes.
    """
    
    res = await client.get(url)

    return res.content

@logger.catch
def get_list_brand(driver: webdriver, url: str) -> None:
    """
    Retrieve the list of brands from the given URL using Selenium WebDriver.
    
    Args:
        driver (webdriver): Selenium WebDriver instance.
        url (str): URL to retrieve the list of brands from.
    
    Returns:
        None
    """
    
    list_all = {}
    try:
        driver.get(url)
        letters = [chr(i) for i in range(ord('A'), ord('Z') + 1)]
        letters.append('#')
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, "content")))
        
        list_index = driver.find_elements(By.CSS_SELECTOR, "div.brand-index ul li span")
        for i in list_index:
            logger.info(f"Now Scraping Capital {i.text}")
            time.sleep(10)
            try:
                
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div.brand-index li")))
                time.sleep(90)
                i.click()
                soup = BeautifulSoup(driver.page_source, "html.parser")
                list_brand = soup.find("div", class_="brand-item")
                list_brand = list_brand.find_all("a")
                list_all[f"{i.text}"] = []
                
                for n in list_brand:                    
                    text = n.get_text(strip=True)
                    href = re.search(r"-(\d+)", n.get("href")).group(1)
                    list_all[f"{i.text}"].append({f"{text}": f"{href}"})
                logger.info(f"Done get brans at Capital {i.text}")
            except ElementClickInterceptedException as e:
                logger.error("apa ini", e.msg, e.args)

            
    except:
        logger.error("an error occurred", e.message)
        
    finally:
        driver.quit()
        return list_all



async def get_all_product_brand(client: httpx.AsyncClient, header: dict) -> None:
    """
    Asynchronously retrieve all product data for brands listed in a JSON file.

    Args:
        client (httpx.AsyncClient): HTTPX asynchronous client instance.
        header (dict): Request headers.

    Returns:
        None
    """
    
    with open("./list_all_brand.json", "r") as f:
        res = json.loads(f.read())
    list_product = {}
    
    MAX_REQUESTS_PER_MINUTE = 100

    for i in res.keys():
        print(i)
        tasks = []
        for n in res[i]['brands']:
            print("header in get_all_product_brand", header)
            tasks.append(get_data_brand(client, n['id'], header))
            
            # If the number of tasks reaches MAX_REQUESTS_PER_MINUTE, gather them and sleep
            if len(tasks) == MAX_REQUESTS_PER_MINUTE:
                gath = await asyncio.gather(*tasks)
                if i not in list_product:
                    list_product[i] = []
                list_product[i].extend(gath)
                tasks = []
                await asyncio.sleep(60)  # Sleep for 60 seconds

        # Gather any remaining tasks in the final batch
        if tasks:
            gath = await asyncio.gather(*tasks)
            if i not in list_product:
                list_product[i] = []
            list_product[i].extend(gath)
    
    
    # REQ_COUNT = 0
    
    # for i in res.keys():
    #     tasks = []
    #     for n in res[i]['brands']:
    #         print("header in get_all_product_brand",header)
    #         tasks.append(get_data_brand(client, n['id'], header))
            
    #     gath = await asyncio.gather(*tasks)
        
    #     list_product[i] = gath
    
    return list_product


async def get_list_brand_httpx(client: httpx.AsyncClient, url: str, header: dict) -> None:
    """
    Asynchronously retrieve the list of brands using HTTPX.

    Args:
        client (httpx.AsyncClient): HTTPX asynchronous client instance.
        url (str): URL to retrieve the list of brands from.
        header (dict): Request headers.

    Returns:
        None
    """
    
    for i in [':authority', ':method', ':path', ':scheme']:
        if i in header:
            del header[i]
    
    char_list = string.ascii_uppercase + "#"
    
    MAX_REQUESTS_PER_MINUTE = 5
    
    list_data_brand = {}
    
    for i in range(0, len(char_list), MAX_REQUESTS_PER_MINUTE):
        batch = char_list[i:i + MAX_REQUESTS_PER_MINUTE]
        tasks = []
        
        for char in batch:
            modified_url = f"{url}?alphabet={char}"
            tasks.append(get_data(client, modified_url, header))
        
        # Gather the results of the current batch
        results = await asyncio.gather(*tasks)
        
        for i, x in enumerate(batch):
            list_data_brand[f"{x}"] = results[i]
        logger.info(f"Making {i} Requests")
        if i + MAX_REQUESTS_PER_MINUTE < len(char_list):
            await asyncio.sleep(60)
            
    return list_data_brand
        


async def get_data(client: httpx.AsyncClient, url: str, header: dict) -> any:
    """
    Asynchronously retrieve JSON data from a URL.

    Args:
        client (httpx.AsyncClient): HTTPX asynchronous client instance.
        url (str): URL to retrieve data from.
        header (dict): Request headers.

    Returns:
        Any: Retrieved JSON data.
    """
    
    res = await client.get(url, headers=header)
    
    return res.json()