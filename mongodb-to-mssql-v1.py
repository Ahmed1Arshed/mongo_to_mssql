import pymongo
import pydash
import traceback
import requests
import json
import pyodbc
import concurrent.futures
from datetime import datetime
from threading import current_thread

client = pymongo.MongoClient("mongodb://localhost:27017/")
mongodb = client["utilities-scraper"]

user = 'api'
password = 'm7boukyvT.TedQ'
database = 'amplify'
server = 'electrical.database.windows.net'

pyodbc.pooling = False

connection_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={user};PWD={password};MultipleActiveResultSets=True "

COMPETITORS = [
    "alliedelec", "baypower", "breakerauthority", "breakerhunter", "breakeroutlet", "chartercontact", "circuitbreaker", "coasttocoastbreaker", "controlparts", "dkhardware", "galco", "gordonelectricsupply", "imc_direct", "radwell", "relectric", "southlandelectrical", "superbreakers", "swgr", "thermaloverloadheatersunits", "zoro",
]

PRODUCT_QUERY = "MERGE INTO scraper_competitor_products\
    AS target\
    USING (SELECT NEWID() AS id, ? AS url, ? AS alternate_names, ? AS captured_at, ? AS competitor, ? AS cutsheet_url, ? AS description, ? AS long_description, ? AS manufacturers, ? AS their_name, CURRENT_TIMESTAMP AS created_at, CURRENT_TIMESTAMP AS updated_at, ? AS extracted_name) AS source (id, url, alternate_names, captured_at, competitor, cutsheet_url, description, long_description, manufacturers, their_name, created_at, updated_at, extracted_name)\
    ON (target.competitor = source.competitor AND target.their_name = source.their_name)\
    WHEN MATCHED THEN UPDATE SET url = ?, alternate_names = ?, competitor = ?, cutsheet_url = ?, description = ?, long_description = ?, manufacturers = ?, their_name = ?, updated_at = CURRENT_TIMESTAMP, extracted_name = ?\
    WHEN NOT MATCHED THEN INSERT (id, url, alternate_names, captured_at, competitor, cutsheet_url, description, long_description, manufacturers, their_name, created_at, updated_at, extracted_name) VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?)\
    OUTPUT INSERTED.ID;"

IMAGE_QUERY = "MERGE INTO scraper_competitor_product_images\
    AS target\
    USING (SELECT NEWID() AS id, ? AS scraper_competitor_product_id, ? AS src, ? AS alt, ? AS title, CURRENT_TIMESTAMP AS created_at, CURRENT_TIMESTAMP AS updated_at) AS source (id, scraper_competitor_product_id, src, alt, title, created_at, updated_at)\
    ON (target.scraper_competitor_product_id = source.scraper_competitor_product_id)\
    WHEN MATCHED THEN UPDATE SET scraper_competitor_product_id = ?, src = ?, alt = ?, title = ?, updated_at = CURRENT_TIMESTAMP\
    WHEN NOT MATCHED THEN INSERT (id, scraper_competitor_product_id, src, alt, title, created_at, updated_at) VALUES (NEWID(), ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)\
    OUTPUT INSERTED.ID;"

OFFER_QUERY = "MERGE INTO scraper_competitor_product_offers\
    AS target\
    USING (SELECT NEWID() AS id, ? AS scraper_competitor_product_id, ? AS condition_id, CURRENT_TIMESTAMP AS created_at, CURRENT_TIMESTAMP AS updated_at, ? AS stock, ? AS price, ? AS currency) AS source (id, scraper_competitor_product_id, condition_id, created_at, updated_at, stock, price, currency)\
    ON (target.scraper_competitor_product_id = source.scraper_competitor_product_id)\
    WHEN MATCHED THEN UPDATE SET scraper_competitor_product_id = ?, condition_id = ?, updated_at = CURRENT_TIMESTAMP, stock = ?, price = ?, currency = ?\
    WHEN NOT MATCHED THEN INSERT (id, scraper_competitor_product_id, condition_id, created_at, updated_at, stock, price, currency) VALUES (NEWID(), ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?)\
    OUTPUT INSERTED.ID;"


def get_new_cursor():
    conn = pyodbc.connect(connection_str)
    return conn.cursor()


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


def ResultIter(competitor, data_size=5000):
    last_doc_id = None
    while True:
        filter = {"competitor": competitor} if last_doc_id is None else {
            "competitor": competitor, "_id": {"$gt": last_doc_id}}
        results = list(mongodb.competitorproducts.find(
            filter).sort("_id", 1).limit(data_size))
        if not results:
            break
        for result in results:
            yield result
        last_doc_id = results[-1]["_id"]


def get_condition_id(condition):
    if condition == "newInBox":
        return "be900e40-8041-464b-951e-c15009df8132"
    elif condition == "newSurplus":
        return "177adc92-f71f-436c-b700-816a438db02a"
    elif condition == "refurbished":
        return "b4a816c9-5604-4578-95f1-57492bc1fb06"
    else:
        return condition


def update_product(cursor, data):
    extracted_names = list_to_csv_string(data["extracted_names"])
    manufacturers = list_to_csv_string(data["manufacturers"])
    captured_at = datetime.strptime(data["captured_at"], '%B %d, %Y %H:%M:%S')
    if data['description'] is not None:
        data['description'] = data['description'].encode('utf8')
    if data['long_description'] is not None:
        data['long_description'] = data['long_description'].encode('utf8')
    if data['url'] is not None:
        data['url'] = data['url'].encode('utf8')
    if data["cutsheet_url"] is not None:
        data["cutsheet_url"] = data["cutsheet_url"].encode('utf8')
    query_args = (
        data["url"], data["alternate_names"], captured_at, data["competitor"], data["cutsheet_url"], data[
            "description"], data["long_description"], manufacturers, data["their_name"], extracted_names,
        data["url"], data["alternate_names"], data["competitor"], data["cutsheet_url"], data["description"], data[
            "long_description"], manufacturers, data["their_name"], extracted_names,
        data["url"], data["alternate_names"], captured_at, data["competitor"], data["cutsheet_url"], data[
            "description"], data["long_description"], manufacturers, data["their_name"], extracted_names
    )
    inserted_id = execute_query(cursor, PRODUCT_QUERY, query_args)
    return inserted_id


def update_images(cursor, data):
    product_id = str(data["product_id"])
    if type(data["images"]) is dict:
        data["images"] = [data["images"]]
    for image in data["images"]:
        query_args = (
            product_id, image["src"].encode('utf8'), image["alt"].encode(
                'utf8'), image["title"].encode('utf8'),
            product_id, image["src"].encode('utf8'), image["alt"].encode(
                'utf8'), image["title"].encode('utf8'),
            product_id, image["src"].encode('utf8'), image["alt"].encode(
                'utf8'), image["title"].encode('utf8')
        )
        execute_query(cursor, IMAGE_QUERY, query_args)


def update_offers(cursor, data):
    product_id = str(data["product_id"])
    for offer in data["offers"]:
        condition_id = get_condition_id(offer["condition"])
        currency = pydash.get(offer, "price.currency", "")
        price = pydash.get(offer, "price.amount", None)
        stock = list_to_csv_string(pydash.get(offer, "stock"))
        query_args = (
            product_id, condition_id, stock, price, currency,
            product_id, condition_id, stock, price, currency,
            product_id, condition_id, stock, price, currency
        )
        execute_query(cursor, OFFER_QUERY, query_args)


def execute_query(cursor, query, query_args):
    try:
        result = cursor.execute(query, query_args).fetchone()[0]
        cursor.commit()
        # print(result)
        return result
    except Exception as e:
        message = "Error: " + str(e) + "\n" + query + "\n" + \
            str(query_args) + "\n" + traceback.format_exc()
        print(message)
        send_slack_message(message)


def from_mongodb_to_mssql(competitor):
    for product in ResultIter(competitor):
        try:
            print(competitor)
            cursor = current_thread().cursor
            product_id = update_product(cursor, product)
            if product_id is None:
                continue
            update_images(
                cursor, {"images": product["images"], "product_id": product_id})
            update_offers(
                cursor, {"offers": product["offers"], "product_id": product_id})
        except Exception as e:
            message = "Error: " + str(e) + "\n" + traceback.format_exc()
            print(message)
            send_slack_message(message)


def initializer_worker():
    current_thread().cursor = get_new_cursor()

now = datetime.now()
start_time = now.strftime("%H:%M:%S")
send_slack_message("Algo start time :"+str(start_time))
executor = concurrent.futures.ThreadPoolExecutor(
    20, initializer=initializer_worker)
for competitor in COMPETITORS:
    executor.submit(from_mongodb_to_mssql, competitor)
