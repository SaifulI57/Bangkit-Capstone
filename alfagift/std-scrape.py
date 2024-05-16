from functionality import *


async def main() -> None:
    # driver: webdriver = init_driver()
    
    
    """
    the code below is to get the fingerprint from the server
    """
    # header = get_header(driver,"adem-sari-12216")
    
    
    
    """
    the code below is to get brand detail and the products from the API
    """
    # async with httpx.AsyncClient() as client:
    #     res = await get_all_product_brand(client, header)
    #     try:
    #         with open("./list_all_product.json", "w") as f:
    #             f.write(json.dumps(res))
    #             f.close()
    #         logger.info("Done scraping list brand at alfagift ✅")
    #     except:
    #         with open("./list_all_product.txt", "w") as f:
    #             f.write(str(res))
    #             f.close()
    
    
    
    list_prod = pd.DataFrame()
    
    
    """
    the code below is to download image from a list of products
    """
    # async with httpx.AsyncClient() as client:
    #     with open("./list_all_product.json", "r") as f:
    #         list_p = json.loads(f.read())
    #         f.close()
    #     for i in list_p:
            # await get_image(client, list_p[i])
        # logger.info("Done Sir")
    
    
    """
    The code below is to convert a list of products into a dataset
    """
    def convert_list_product_to_csv() -> None:
        with open("./list_all_product.json", "r") as f:
            list_p = json.loads(f.read())
            f.close()
        for i in list_p:
            res = get_df(list_p[i])
            res.set_index("id")
            if list_prod.empty:
                list_prod = res
            else:
                list_prod = pd.concat([list_prod, res], ignore_index=False)
                
        
        df = list_prod.set_index("id")
        df.to_csv("./dataset_alfagift.csv")
    
    """
    the code below is to get a list of brands from the API
    """
    # async with httpx.AsyncClient() as client:
    #     res = await get_list_brand_httpx(client, "https://webcommerce-gw.alfagift.id/v2/brands", header)
    #     try:
    #         with open("./list_all_brand.json", "w") as f:
    #             f.write(json.dumps(res))
    #             f.close()
    #         logger.info("Done scraping list brand at alfagift ✅")
    #     except:
    #         with open("./list_all_brand.txt", "w") as f:
    #             f.write(str(res))
    #             f.close()
                
    #         logger.info("There's an error write data to json file, write to txt instead...")
    
    
    """
    The code below is to sort downloaded images
    """
    # sort_directory()


if __name__ == "__main__":
    asyncio.run(main())