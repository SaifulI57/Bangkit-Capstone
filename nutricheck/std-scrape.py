import pandas as pd
import redis
import httpx
from typing import Any
import asyncio

MAX_PER_REQUEST = 100


# TODO: 
# 1. Enumerate all the endpoint links.
# 2. Filter and organize the links related to "Siap Saji" (code xSxxx).
# 3. Retrieve data from all the filtered links.
# 4. Transform the retrieved data into a DataFrame format.
# 5. Optionally, store the data in Redis as a backup in case of conversion errors.

# Note:
# - The database at nutricheck.id contains a total of 4104 records.
# - Codes starting with xMxxx represent "Mentah" (Raw).
# - Codes starting with xOxxx represent "Olahan" (Processed).
# - Codes starting with xSxxx represent "Siap Saji" (Ready-to-Eat).


urlPatternNutrient = "https://nutricheck.id/index.php/bahan/selectedBahan?id={}"
urlProduct ="https://nutricheck.id/index.php/bahan/get"
path = "./dataset_nutricheck.csv"


async def getProduct(client: httpx.AsyncClient, url) -> Any:
    res = await client.get(url)
    return res.json()


async def getData(client: httpx.AsyncClient, url) -> Any:
    while True:
        try:
            resp = await client.get(url)
            return resp.json()['data']
        except Exception as e:
            print(f"Error occurred while requesting data: {e}")
            print("Retrying in 5 minutes...")
            await asyncio.sleep(300)

def to_df(dict):
    if dict == None:
        return None
    df = pd.DataFrame.from_dict(dict)
    df.rename_axis('no',inplace=True)
    print(df)
    df['brand'] = df['nama_bahan'].apply(lambda x: x.split(', ')[0] or x)
    df['nama_product'] = df['nama_bahan'].apply(lambda x: ''.join(x.split(', ')[1::]) or x)
    
    df.loc[df['kode'].str.contains('S'),'type'] = 'Siap Saji'
    return df

async def fetch() -> Any:
    async with httpx.AsyncClient() as client:
        product = await getProduct(client, urlProduct)
        counter = 0
        total_requests = 0
        product = [item for item in product if 'S' in item['kode']]
        for i in range(0, (len(product) // MAX_PER_REQUEST) + 1):
            try:
                csvs = pd.read_csv(path)
                csvs.set_index('no',inplace=True)
            except:
                csvs = pd.DataFrame()
                print('no data found')
            request_made = 0
            reqs = []
            for item in product:
                try:
                    if str(item['id']) in str(csvs['id'].values.tolist()):
                        continue
                except:
                    None
                counter += 1
                total_requests += 1
                print('total requests ', total_requests)
                if counter >= MAX_PER_REQUEST:
                    break
                reqs.append(getData(client, urlPatternNutrient.format(item['id'])))
                request_made += 1
                if request_made >= 10:
                    print('sleep 20s')
                    await asyncio.sleep(20)
                    request_made = 0
            result = await asyncio.gather(*reqs)
            result = to_df(result)
            try:
                csvs = pd.concat([csvs,result], ignore_index=True)
            except:
                csvs = result
            csvs.rename_axis('no',inplace=True)
            csvs.to_csv(path)
            counter = 0
asyncio.run(fetch())