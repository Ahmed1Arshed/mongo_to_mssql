import os
import pymongo
import pydash
import traceback
import requests
import json
import pyodbc
import uuid
import concurrent.futures
from datetime import datetime
from threading import current_thread
client = pymongo.MongoClient("mongodb://localhost:27017/")
mongodb = client["utilities-scraper"]
# user = 'sa'
# password = 'f@izan786'
# database = 'testdb'
# server = 'localhost'
user = 'api'
password = 'm7boukyvT.TedQ'
database = 'amplify'
server = 'electrical.database.windows.net'
pyodbc.pooling = False
connection_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={user};PWD={password};MultipleActiveResultSets=True;"
COMPETITORS = [
    "alliedelec", "baypower", "breakerauthority", "breakerhunter", "breakeroutlet", "chartercontact", "circuitbreaker", "coasttocoastbreaker", "controlparts",
    "dkhardware", "galco", "gordonelectricsupply", "imc_direct", "radwell", "relectric", "southlandelectrical", "superbreakers", "swgr", "thermaloverloadheatersunits", "zoro",
]
DOCS_TO_SKIP = 0
DOCS_CHUNK_SIZE = 10000
INSERT_PRODUCT = "INSERT INTO scraper_competitor_products_new (id, url, alternate_names, captured_at, competitor, cutsheet_url, description, long_description, manufacturers, their_name, created_at, updated_at, extracted_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?);"
INSERT_IMAGE = "INSERT INTO scraper_competitor_product_images_new (id, scraper_competitor_product_id, src, alt, title, created_at, updated_at) VALUES (NEWID(), ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);"
INSERT_OFFER = "INSERT INTO scraper_competitor_product_offers_new (id, scraper_competitor_product_id, condition_id, created_at, updated_at, stock, price, currency) VALUES (NEWID(), ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?);"
def get_new_cursor():
    conn = pyodbc.connect(connection_str)
    conn.autocommit = True
    return conn.cursor()
def get_docs_to_skip():
    return DOCS_TO_SKIP
def set_docs_to_skip(count):
    global DOCS_TO_SKIP
    DOCS_TO_SKIP += count
    with open("docs_to_skip.jl", "w+") as file:
        json.dump({"docs_to_skip": DOCS_TO_SKIP}, file)
def get_docs_to_skip_from_json_file():
    if os.path.exists("./docs_to_skip.jl"):
        with open("docs_to_skip.jl") as file:
            for line in file:
                result = json.loads(line)
                set_docs_to_skip(result["docs_to_skip"])
def send_slack_message(message):
    payload = json.dumps({"text": message.replace('"', "'")})
    response = requests.post(
        'https://hooks.slack.com/services/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', data=payload)
    return response
def list_to_csv_string(li):
    if li is None or type(li) is str:
        return li
    if type(li) is list:
        csv_string = ""
        for item in li:
            csv_string += f",{item}" if len(
                csv_string) > 0 else f"{item}"
        return csv_string
def split_in_chunks(a, n):
    k, m = divmod(len(a), n)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))
def get_products_chunk_from_mongo(data_size=DOCS_CHUNK_SIZE):
    try:
        docs_to_skip = get_docs_to_skip()
        results = list(mongodb.competitorproducts.find().sort(
            "_id", 1).skip(docs_to_skip).limit(data_size))
        print("Docs to skip: ", docs_to_skip,
              " Results length: ", len(results))
        return results
    except Exception as e:
        message = "Error: " + str(e) + "\n" + traceback.format_exc()
        send_slack_message(message)
        print(message)
def get_condition_id(condition):
    if condition in ["newInBox", "newIndBox"]:
        return "be900e40-8041-464b-951e-c15009df8132"
    elif condition == "newSurplus":
        return "177adc92-f71f-436c-b700-816a438db02a"
    elif condition == "refurbished":
        return "b4a816c9-5604-4578-95f1-57492bc1fb06"
    elif condition == "used":
        return "5ce27e59-c7ca-4baa-9368-15dac5d3b180"
    else:
        return condition
def insert_products(cursor, products_to_insert):
    try:
        cursor.executemany(INSERT_PRODUCT, products_to_insert)
    except Exception as e:
        message = "Error: " + str(e) + "\n" + traceback.format_exc()
        send_slack_message(message)
        print(message)
def insert_images(cursor, images_to_insert):
    try:
        cursor.executemany(INSERT_IMAGE, images_to_insert)
    except Exception as e:
        message = "Error: " + str(e) + "\n" + traceback.format_exc()
        send_slack_message(message)
        print(message)
def insert_offers(cursor, offers_to_insert):
    try:
        cursor.executemany(INSERT_OFFER, offers_to_insert)
    except Exception as e:
        message = "Error: " + str(e) + "\n" + traceback.format_exc()
        send_slack_message(message)
        print(message)
def process_data(data):
    products_to_insert = []
    images_to_insert = []
    offers_to_insert = []
    for item in data:
        try:
            item["extracted_names"] = list_to_csv_string(
                item["extracted_names"])
            item["manufacturers"] = list_to_csv_string(item["manufacturers"])
            item["captured_at"] = datetime.strptime(
                item["captured_at"], '%B %d, %Y %H:%M:%S')
            item["id"] = str(uuid.uuid4())
            products_to_insert.append([
                item["id"], item["url"], item["alternate_names"], item["captured_at"], item["competitor"], item["cutsheet_url"], item[
                    "description"], item["long_description"], item["manufacturers"], item["their_name"], item["extracted_names"]
            ])
            if type(item["images"]) is dict:
                item["images"] = [item["images"]]
            for image in item["images"]:
                images_to_insert.append([
                    item["id"], image["src"],
                    image["alt"], image["title"]
                ])
            for offer in item["offers"]:
                offers_to_insert.append([
                    item["id"], get_condition_id(offer["condition"]),
                    list_to_csv_string(pydash.get(offer, "stock")),
                    pydash.get(offer, "price.amount", None),
                    pydash.get(offer, "price.currency", "")
                ])
        except Exception as e:
            message = "Error: " + str(e) + "\n" + traceback.format_exc()
            send_slack_message(message)
            print(message)
    return {
        "products_to_insert": products_to_insert,
        "images_to_insert": images_to_insert,
        "offers_to_insert": offers_to_insert
    }
def save_data_to_mssql(products):
    try:
        cursor = current_thread().cursor
        cursor.fast_executemany = True
        data = process_data(products)
        insert_products(cursor, data["products_to_insert"])
        insert_images(cursor, data["images_to_insert"])
        insert_offers(cursor, data["offers_to_insert"])
    except Exception as e:
        message = "Error: " + str(e) + "\n" + traceback.format_exc()
        send_slack_message(message)
        print(message)
def initializer_worker():
    current_thread().cursor = get_new_cursor()
def main():
    get_docs_to_skip_from_json_file()
    executor = concurrent.futures.ThreadPoolExecutor(
        20, initializer=initializer_worker)
    products = get_products_chunk_from_mongo()
    while len(products) > 0:
        products_chunks = split_in_chunks(products, 20)
        futures = [executor.submit(save_data_to_mssql, chunk)
                   for chunk in products_chunks]
        concurrent.futures.wait(futures)
        set_docs_to_skip(DOCS_CHUNK_SIZE)
        if len(products) < DOCS_CHUNK_SIZE:
            break
        products = get_products_chunk_from_mongo()
    set_docs_to_skip(0)
if __name__ == '__main__':
    main()