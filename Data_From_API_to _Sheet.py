import aiohttp
import asyncio
import json
import pandas as pd 
from warnings import filterwarnings
filterwarnings('ignore')
import nest_asyncio
import re
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account


def remove_list(value):
    if isinstance(value, list):
        return ', '.join(map(str, value))
    return value


def splits(s):
    split_result = re.split(':', s)
    if len(split_result) > 1:
        return split_result[1]
    else:
        return ""


def splits2(s):
    split_result = re.split(':', s)
    if len(split_result) > 2:
        return split_result[2]
    else:
        return ""    



def splits3(s):
    split_result = re.split(':', s)
    if len(split_result) >= 0:
        return split_result[0]
    else:
        return ""    
#-------------------------------------
    
def splitss2(s):
    split_result = re.split(',', s)
    if len(split_result) > 1:
        return split_result[1]
    else:
        return ""


def splitss3(s):
    split_result = re.split(',', s)
    if len(split_result) > 2:
        return split_result[2]
    else:
        return ""    

    

def splitss(s):
    split_result = re.split(',', s)
    if len(split_result) >= 0:
        return split_result[0]
    else:
        return ""       
    
import numpy as np
def testing(d):
    qt  = []
    sku = []
    for i in d.columns:
        qty = pd.json_normalize(data=d[i]).add_prefix(i)
        qt.extend(qty.iloc[:,[7]].values)
        sku.extend(qty.iloc[:,[15]].values)
    return pd.concat([pd.DataFrame(sku,columns=['SKU']),pd.DataFrame(qt,columns=['PO Pending'])],axis=1).dropna()

    

nest_asyncio.apply()

access_token = '5538c089-dd35-4f61-98f0-90097d58621e'
headers = {'Authorization': f'Bearer {access_token}'}

async def fetch_inventory(session, page):
    url = f'https://api.skubana.com/v1/inventory?limit=100&page={page}'
    #f'https://api.skubana.com/v1.1/productstocks/total?limit=100&page={page}'
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            return await response.json()
        else:
            print(f'Request failed with status code {response.status}: {response.text}')
            return []

async def get_all_inventory():
    async with aiohttp.ClientSession() as session:
        inventory_list = []
        for page in range(1, 60):
            inventory_data = await fetch_inventory(session, page)
            inventory_list.extend(inventory_data)
            await asyncio.sleep(1)  # introduce a 2-second delay
        return inventory_list

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    inventory_data = loop.run_until_complete(get_all_inventory())
    print(len(inventory_data))


nest_asyncio.apply()

access_token = ''
headers = {'Authorization': f'Bearer {access_token}'}

async def fetch_inventory(session, page):
    url = f'https://api.skubana.com/v1.1/purchaseorders?limit=50&page={page}'
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            return await response.json()
        else:
            print(f'Request failed with status code {response.status}: {response.text}')
            return []

async def get_all_inventory():
    async with aiohttp.ClientSession() as session:
        inventory_list_po = []
        for page in range(1, 40):
            inventory_data_po = await fetch_inventory(session, page)
            inventory_list_po.extend(inventory_data_po)
            await asyncio.sleep(1)  # introduce a 2-second delay
        return inventory_list_po

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    inventory_data_po = loop.run_until_complete(get_all_inventory())
    print(len(inventory_data_po))
 
    
    
    
async def fetch_inventory(session, page):
    url = f'https://api.skubana.com/v1.1/products?limit=100&page={page}'
    #f'https://api.skubana.com/v1.1/productstocks/total?limit=100&page={page}'
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            return await response.json()
        else:
            print(f'Request failed with status code {response.status}: {response.text}')
            return []

async def get_all_inventory():
    async with aiohttp.ClientSession() as session:
        inventory_product = []
        for page in range(1, 60):
            inventory_p_data = await fetch_inventory(session, page)
            inventory_product.extend(inventory_p_data)
            await asyncio.sleep(1)  # introduce a 2-second delay
        return inventory_product

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    inventory_p_data = loop.run_until_complete(get_all_inventory())
    print(len(inventory_p_data))    
 



      
    json_strr = json.dumps(inventory_data)
    json_strrr = json.dumps(inventory_data_po)
    json_str = json.dumps(inventory_p_data)

df = pd.read_json(json_strr, orient='records')
product_data = pd.json_normalize(data=df["product"]).add_prefix("product--")
stockLocation_data = pd.json_normalize(data=df["stockLocation"]).add_prefix("stockLocation--")
warehouseStockTotals_data = pd.json_normalize(data=df["warehouseStockTotals"]).add_prefix("warehouseStockTotals--")
# merge the original DataFrame and the normalized DataFrame
df_normalize = pd.concat([df.drop(columns=["product","stockLocation","warehouseStockTotals"]), 
                          product_data,stockLocation_data,warehouseStockTotals_data], axis=1)


dd = df_normalize[["quantity","availableQuantity","product--masterSku","product--name","stockLocation--location","warehouseStockTotals--availableQuantity"]]
dd["stockLocation--location"].replace({"GLOBAL":"Warehouse","NYC":"Our Office"},inplace=True)
# Pivot the DataFrame and sum the values
df_pivot = dd.pivot_table(index=['product--masterSku', 'product--name',"quantity"], columns='stockLocation--location', values='warehouseStockTotals--availableQuantity', aggfunc='first').reset_index()

# Rename columns
df_pivot.columns.name = None
df_pivot = df_pivot.rename(columns={'Warehouse': 'NYC', 'Our Office': 'Global'})

# Add new column 'AOHTOTAL' which is the sum of 'NYC' and 'Global' columns
df_pivot['AOHTOTAL'] = df_pivot['NYC'].fillna(0) + df_pivot['Global'].fillna(0)

# Reorder columns
df_pivot = df_pivot[['product--masterSku','Global','NYC','AOHTOTAL','product--name',"quantity"]]

# Print the result
df_pivot.rename(columns = {'NYC':'AOH WP(global)','Global':'AOH NYC','AOHTOTAL':'AOH Total','product--masterSku':'SKU','product--name':'name',"quantity":"QuantityNYC"}, inplace = True)
df_pivot["AOH WP(global)"] = df_pivot["AOH WP(global)"].fillna(0)
df_pivot.drop("name",axis=1,inplace=True)
df_pivot = df_pivot[~df_pivot['SKU'].str.startswith('IFERROR')]
df_pivot = df_pivot[~df_pivot['SKU'].str.startswith('WRONG')]
df_pivot = df_pivot.reset_index(drop=True)

dff = pd.read_json(json_str, orient='records')
dff.rename(columns = {'masterSku':'SKU'},inplace = True)
data = dff[['SKU',"name","categories",'type', 'brand']]

data["Colour&Size"] = data["name"].apply(splits)
data["Carret"] = data["name"].apply(splits2)
data["name"] = data["name"].apply(splits3)


merrr = pd.merge(data,
                 df_pivot,
                 on='SKU',
                 how='left')

merrr['categories'] = merrr['categories'].apply(lambda x: remove_list(x))
daa = merrr[["SKU","AOH NYC","AOH WP(global)","AOH Total","name","Colour&Size","Carret",'categories',"QuantityNYC","type","brand"]]

daa["AOH NYC"] = daa["AOH NYC"].fillna(0)
daa["AOH WP(global)"] = daa["AOH WP(global)"].fillna(0)
daa["AOH Total"] = daa["AOH Total"].fillna(0)
daa = daa[~daa['SKU'].str.startswith('IFERROR')]
daa = daa[~daa['SKU'].str.startswith('WRONG')]
daa = daa.reset_index(drop=True)


d_f = pd.read_json(json_strrr, orient='records')

dat = d_f[d_f.status == "PENDING_DELIVERY"]
d = pd.json_normalize(data=dat["purchaseOrderItems"]).add_prefix("purchaseOrderItems--")
testing(d)['PO Pending'] = testing(d)['PO Pending'].fillna(0)

dataset = testing(d).reset_index(drop=True)

data_set = pd.merge(daa,
                    dataset,
                    on='SKU',
                    how='left')

data_set['PO Pending'] = data_set['PO Pending'].fillna(0)
data_set.rename(columns = {'SKU':'Sku','AOH WP(global)':'AOH WP',"name":"Name","Colour&Size":"Colour Size","Carret":"Carat","categories":"Categories"},inplace=True)
data_set = data_set[["Sku","AOH NYC",'AOH WP',"AOH Total","Name","Colour Size","Carat",'PO Pending',"Categories","QuantityNYC","type","brand"]]
data_set["Categories1"] = data_set["Categories"].apply(splitss)
data_set["Categories2"] = data_set["Categories"].apply(splitss2)
data_set["Categories3"] = data_set["Categories"].apply(splitss3)
data_set.drop("Categories",axis=1,inplace=True)
data_set = data_set.groupby('Sku').agg({
    'AOH NYC': 'sum',
    'AOH WP': 'sum',
    'AOH Total': 'sum',
    'Name': lambda x: x.mode().iat[0],
    'Colour Size': lambda x: x.mode().iat[0],
    'Carat': lambda x: x.mode().iat[0],
    'type': lambda x: x.mode().iat[0],
    'brand': lambda x: x.mode().iat[0],
    'QuantityNYC': 'sum',
    'Categories1': lambda x: x.mode().iat[0],
    'Categories2': lambda x: x.mode().iat[0],
    'Categories3': lambda x: x.mode().iat[0]
}).reset_index()


dataset.rename(columns = {'SKU':'Sku'},inplace=True)
data_set.rename(columns = {"QuantityNYC":'Kit packed NYC',"type":"Type","brand":"Brand"},inplace=True)
data_set = pd.merge(data_set,
                    dataset,
                    on='Sku',
                    how='left')
data_set['PO Pending'] = data_set['PO Pending'].fillna(0)

data_set = data_set[["Sku","AOH NYC","AOH WP","AOH Total","PO Pending","Kit packed NYC","Name","Carat","Colour Size","Categories1","Categories2","Categories3","Brand","Type"]]
data_set["Kit packed NYC"] = data_set.apply(lambda row: row["Kit packed NYC"] if row["Type"] == "BUNDLE_KIT" else 0, axis=1) 


mask = data_set["Kit packed NYC"] > 0
data_set.loc[mask, "AOH NYC"] = data_set.loc[mask, "AOH NYC"] - data_set.loc[mask,"Kit packed NYC"]

# Service account credentials
credentials = service_account.Credentials.from_service_account_file(
    r'/home/ahsanulhaq4222/impressive-mile-387209-69572dc70a0b.json',
    scopes=['https://www.googleapis.com/auth/spreadsheets']
)

# Create a Google Sheets API client
service = build('sheets', 'v4', credentials=credentials)


# Get the column names
column_names = data_set.columns.values.tolist()

# Convert the DataFrame to a list of lists
data_set.fillna('', inplace=True)
values = [column_names] + data_set.values.tolist()

# Spreadsheet ID of the target Google Sheet
spreadsheet_id = ''

# Sheet name to upload the data to
sheet_name = 'AOHTOTAL'

# Clear the existing data in the sheet
service.spreadsheets().values().clear(
   spreadsheetId=spreadsheet_id,
   range=sheet_name
).execute()

# Upload the data to the sheet
service.spreadsheets().values().update(
   spreadsheetId=spreadsheet_id,
   range=sheet_name,
   valueInputOption='RAW',
   body={'values': values}
).execute()

print('Excel file uploaded successfully.')
