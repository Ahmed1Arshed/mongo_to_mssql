import time
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

user = 'api'
password = 'm7boukyvT.TedQ'
database = 'amplify'
server = 'electrical.database.windows.net'

pyodbc.pooling = False

connection_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={user};PWD={password};MultipleActiveResultSets=True "

COMPETITORS = [
    "alliedelec", "baypower", "breakerauthority", "breakerhunter", "breakeroutlet", "chartercontact", "circuitbreaker", "coasttocoastbreaker", "controlparts", "dkhardware", "galco", "gordonelectricsupply", "imc_direct", "radwell", "relectric", "southlandelectrical", "superbreakers", "swgr", "thermaloverloadheatersunits", "zoro",
]

DOCS_TO_SKIP = {
    "alliedelec": 0, "baypower": 0, "breakerauthority": 0, "breakerhunter": 0, "breakeroutlet": 0,
    "chartercontact": 0, "circuitbreaker": 0, "coasttocoastbreaker": 0, "controlparts": 0,
    "dkhardware": 0, "galco": 0, "gordonelectricsupply": 0, "imc_direct": 0, "radwell": 0,
    "relectric": 0, "southlandelectrical": 0, "superbreakers": 0, "swgr": 0, "thermaloverloadheatersunits": 0, "zoro": 0
}

SELECT_PRODUCTS = "SELECT * FROM scraper_competitor_products WHERE competitor='{0}' AND their_name IN ({1});"

INSERT_PRODUCT = "INSERT INTO scraper_competitor_products (id, url, alternate_names, captured_at, competitor, cutsheet_url, description, long_description, manufacturers, their_name, created_at, updated_at, extracted_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?);"

UPDATE_PRODUCT = "UPDATE scraper_competitor_products SET url = ?, alternate_names = ?, competitor = ?, cutsheet_url = ?, description = ?, long_description = ?, manufacturers = ?, their_name = ?, updated_at = CURRENT_TIMESTAMP, extracted_name = ?\
    WHERE competitor = ? AND their_name = ?;"

SELECT_IMAGES = "SELECT * FROM scraper_competitor_product_images WHERE scraper_competitor_product_id in (?);"

INSERT_IMAGE = "INSERT INTO scraper_competitor_product_images (id, scraper_competitor_product_id, src, alt, title, created_at, updated_at) VALUES (NEWID(), ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);"

UPDATE_IMAGE = "UPDATE scraper_competitor_product_images SET src = ?, alt = ?, title = ?, updated_at = CURRENT_TIMESTAMP\
    WHERE scraper_competitor_product_id = ?;"

SELECT_IMAGES = "SELECT * FROM scraper_competitor_product_offers WHERE scraper_competitor_product_id in (?);"

INSERT_OFFER = "INSERT INTO scraper_competitor_product_offers (id, scraper_competitor_product_id, condition_id, created_at, updated_at, stock, price, currency) VALUES (NEWID(), ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?);"

UPDATE_OFFER = "UPDATE scraper_competitor_product_offers SET condition_id = ?, updated_at = CURRENT_TIMESTAMP, stock = ?, price = ?, currency = ?\
    WHERE scraper_competitor_product_id = ?;"


def get_new_cursor():
    conn = pyodbc.connect(connection_str)
    conn.autocommit = True
    return conn.cursor()


def get_docs_to_skip(competitor):
    return DOCS_TO_SKIP[competitor]


def set_docs_to_skip(competitor, count):
    DOCS_TO_SKIP[competitor] += count


def send_slack_message(message):
    payload = json.dumps({"text": message.replace('"', "'")})
    response = requests.post(
        'https://hooks.slack.com/services/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', data=payload)
    return response


def list_to_sql_list(li):
    sqllist = "%s" % "\",\"".join(li)
    return sqllist


def list_to_csv_string(li):
    if li is None or type(li) is str:
        return li
    if type(li) is list:
        csv_string = ""
        for item in li:
            csv_string += f",{item}" if len(
                csv_string) > 0 else f"{item}"
        return csv_string


def get_products_from_mongo(competitor, data_size=1000):
    try:
        docs_to_skip = get_docs_to_skip(competitor)
        results = list(mongodb.competitorproducts.find(
            {"competitor": competitor}).skip(docs_to_skip).limit(data_size))
        print("Competitor: ", competitor, " Docs to skip: ",
            docs_to_skip, " Results length: ", len(results), "\n\n")
        set_docs_to_skip(competitor, data_size)
        return results
    except Exception as e:
        message = "Error: " + str(e) + "\n" + traceback.format_exc()
        send_slack_message(message)
        print(message)


def get_products_from_mssql(cursor, products):
    try:
        their_names = pydash.uniq(pydash.map_(products, lambda x: f"'{x['their_name']}'"))
        query = SELECT_PRODUCTS.format(products[0]["competitor"], ', '.join('?' * len(their_names)))
        ms_db_products = cursor.execute(query, *their_names).fetchall()
        return ms_db_products
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
    else:
        return condition


def upsert_products(cursor, products_to_insert, products_to_update):
    try:
        if len(products_to_insert) > 0:
            cursor.executemany(INSERT_PRODUCT, products_to_insert)
        if len(products_to_update) > 0:
            cursor.executemany(UPDATE_PRODUCT, products_to_insert)
        # cursor.commit()
    except Exception as e:
        message = "Error: " + str(e) + "\n" + traceback.format_exc()
        send_slack_message(message)
        print(message)


def upsert_images(cursor, images_to_insert, images_to_update):
    try:
        if len(images_to_insert) > 0:
            cursor.executemany(INSERT_IMAGE, images_to_insert)
        if len(images_to_update) > 0:
            cursor.executemany(UPDATE_IMAGE, images_to_insert)
        # cursor.commit()
    except Exception as e:
        message = "Error: " + str(e) + "\n" + traceback.format_exc()
        send_slack_message(message)
        print(message)


def upsert_offers(cursor, offers_to_insert, offers_to_update):
    try:
        if len(offers_to_insert) > 0:
            cursor.executemany(INSERT_OFFER, offers_to_insert)
        if len(offers_to_update) > 0:
            cursor.executemany(UPDATE_OFFER, offers_to_insert)
        # cursor.commit()
    except Exception as e:
        message = "Error: " + str(e) + "\n" + traceback.format_exc()
        send_slack_message(message)
        print(message)


def process_data(db_results, data):
    products_to_insert = []
    products_to_update = []
    images_to_insert = []
    images_to_update = []
    offers_to_insert = []
    offers_to_update = []
    for item in data:
        try:
            item["extracted_names"] = list_to_csv_string(item["extracted_names"])
            item["manufacturers"] = list_to_csv_string(item["manufacturers"])
            item["captured_at"] = datetime.strptime(
                item["captured_at"], '%B %d, %Y %H:%M:%S')
            filter = {"competitor": item["competitor"],
                    "their_name": item["their_name"]}
            product_id = pydash.get(pydash.filter_(db_results, filter), "[0].id")
            if db_results is not None and product_id is not None:
                products_to_update.append([
                    item["url"], item["alternate_names"], item["competitor"], item["cutsheet_url"], item["description"],
                    item["long_description"], item["manufacturers"], item["their_name"], item["extracted_names"], item["competitor"], item["their_name"]
                ])
                if type(item["images"]) is dict:
                    item["images"] = [item["images"]]
                for image in item["images"]:
                    images_to_update.append([
                        image["src"], image["alt"],
                        image["title"], product_id
                    ])
                for offer in item["offers"]:
                    offers_to_update.append([
                        get_condition_id(offer["condition"]),
                        list_to_csv_string(pydash.get(offer, "stock")),
                        pydash.get(offer, "price.amount", None),
                        pydash.get(offer, "price.currency", ""),
                        product_id
                    ])
            else:
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
        "products_to_insert": products_to_insert, "products_to_update": products_to_update,
        "images_to_insert": images_to_insert, "images_to_update": images_to_update,
        "offers_to_insert": offers_to_insert, "offers_to_update": offers_to_update
    }


def save_data_to_mssql(products):
    try:
        cursor = current_thread().cursor
        # cursor = get_new_cursor()
        ms_db_products = get_products_from_mssql(cursor, products)
        data = process_data(ms_db_products, products)
        upsert_products(
            cursor, data["products_to_insert"], data["products_to_update"])
        upsert_images(cursor, data["images_to_insert"],
                      data["images_to_update"])
        upsert_offers(cursor, data["offers_to_insert"],
                      data["offers_to_update"])
        # time.sleep(10)
    except Exception as e:
        message = "Error: " + str(e) + "\n" + traceback.format_exc()
        send_slack_message(message)
        print(message)


def from_mongodb_to_mssql(competitor):
    try:
        print("getting products...")
        products = get_products_from_mongo(competitor)
        while len(products) > 0:
            save_data_to_mssql(products)
            products = get_products_from_mongo(competitor)
    except Exception as e:
        message = "Error: " + str(e) + "\n" + traceback.format_exc()
        send_slack_message(message)
        print(message)


def initializer_worker():
    current_thread().cursor = get_new_cursor()


def main():
    now = datetime.now()
    start_time = now.strftime("%H:%M:%S")
    send_slack_message("Algo start time :"+str(start_time))
    executor = concurrent.futures.ThreadPoolExecutor(
        20, initializer=initializer_worker)
    for competitor in COMPETITORS:
        executor.submit(from_mongodb_to_mssql, competitor)


if __name__ == '__main__':
    main()
