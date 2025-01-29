import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id: str, client_id: str, seller_token: str) -> list:
    """Get a list of OZON store products.

    Args:
        last_id (str): ID of the last value on the page.
            An empty value for the first request.
        client_id (str): OZON API client ID.
        seller_token (str): OZON API Key.

    Returns:
        list: Returns the result as a list of products.

    Example:
        Request:

            {
                "filter": {
                    "offer_id": [
                        "136748"
                    ],
                    "product_id": [
                        "223681945"
                    ],
                    "visibility": "ALL"
                },
                "last_id": "",
                "limit": 100
            }

        Response:

            {
                "result": {
                    "items": [
                        {
                            "product_id": 223681945,
                            "offer_id": "136748"
                        }
                    ],
                    "total": 1,
                    "last_id": "bnVсbA=="
                }
            }

    Raises:
        Requests.exceptions.HTTPError

    """

    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id: str, seller_token: str) -> list:
    """Get the IDs of the OZON store's products.

    Args:
        client_id (str): OZON API client ID.
        seller_token (str): OZON API Key.

    Returns:
        list: Returns a list of product IDs.

    """

    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id: str, seller_token: str) -> str:
    """Update prices for the OZON trading platform.

    Args:
        prices (list): List with actual prices.
        client_id (str): OZON API client ID.
        seller_token (str): OZON API Key.

    Returns:
        str: Returns update data of prices.

    Example:
        Request:

            {
                "prices": [
                    {
                        "auto_action_enabled": "UNKNOWN",
                        "currency_code": "RUB",
                        "min_price": "800",
                        "min_price_for_auto_actions_enabled": true,
                        "offer_id": "",
                        "old_price": "0",
                        "price": "1448",
                        "price_strategy_enabled": "UNKNOWN",
                        "product_id": 1386,
                        "quant_size": 1,
                        "vat": "0.1"
                    }
                ]
            }

        Response:

            {
                "result": [
                    {
                        "product_id": 55946,
                        "offer_id": "PG-2404С1",
                        "updated": true,
                        "errors": []
                    }
                ]
            }

    Raises:
        Requests.exceptions.HTTPError

    """

    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id: str, seller_token: str) -> str:
    """Update watch stocks.

    Args:
        stocks (list): Old list with watch stocks.
        client_id (str): OZON API client ID.
        seller_token (str): OZON API Key.

    Returns:
        str: Returns update data of stocks.

    Example:
        Request:

            {
                "stocks": [
                    {
                        "offer_id": "PG-2404С1",
                        "product_id": 55946,
                        "stock": 4
                    }
                ]
            }

        Response:

        {
            "result": [
            {
                "product_id": 55946,
                "offer_id": "PG-2404С1",
                "updated": true,
                "errors": []
            }
            ]
        }

    Raises:
        Requests.exceptions.HTTPError

    """

    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock() -> dict:
    """Downloads the leftover file.

    Returns:
        dict: Returns a watch remnants.

    """

    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants: dict, offer_ids: list) -> list:
    """Create a stock list.

    Args:
        watch_remnants (dict): dictionary with the number
            of remaining watch on the site.
        offer_ids (list): list of product IDs.

    Returns:
        list: Returns a stocks.

    """

    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants: dict, offer_ids: list) -> list:
    """Create prices for the OZON trading platform.

    Args:
        watch_remnants (dict): Number of remaining watch on the site.
        offer_ids (list): List of product IDs.

    Returns:
        list: Returns a list of prices.

    """

    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Converts the price to an integer.
    
    Args:
        price (str): the date of the price to be converted.
        
    Returns:
        str: Returns the converted price string.
        
    Example:
        >>> print(price_conversion("5'990.00 руб."))
        "5990"
    
    """

    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int) -> list:
    """Divide the list into n elements.

    Args:
        lst (list): The list that needs to be divided.
        n (int): The number of elements for each iteration of the division.

    Yields:
        list: Returns a next list of values with a range of n.

    """

    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants: dict, client_id: str, seller_token: str) -> list:
    """Upload actual prices.

    Args:
        watch_remnants (dict): Number of remaining watch on the site.
        client_id (str): OZON API client ID.
        seller_token (str): OZON API Key.

    Returns:
        list: Returns a list of prices.

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants: dict, client_id: str, seller_token: str):
    """Upload actual stocks.

    Args:
        watch_remnants (dict): Number of remaining watch on the site.
        client_id (str): OZON API client ID.
        seller_token (str): OZON API Key.

    Returns:
        not_empty (list): Returns a list of non-zero stocks.
        stocks (list): Returns a list of actual stocks.

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
