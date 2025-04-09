"""
Digimon Card Database Creator
Author: Deckoner
"""

# Standard library imports
import os
import csv
import re
from concurrent.futures import ThreadPoolExecutor

# Third party imports
import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from PIL import Image
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

load_dotenv()


def create_csv(bt_pages=None):
    """
    Cooks up a fresh CSV with card data from the Digimon website.

    Args:
        bt_pages (list, optional): Specific BT sets to process.
        If not provided, grabs all available BTs from the website.
    """
    if bt_pages is None:
        bt_pages = _list_BTs()

    headers = [
        "card_number",
        "name",
        "card_type",
        "rarity",
        "color_one",
        "color_two",
        "color_three",
        "image_url",
        "cost",
        "stage",
        "attribute",
        "type_one",
        "type_two",
        "evolution_cost_one",
        "evolution_cost_two",
        "effect",
        "evolution_effect",
        "security_effect",
        "bt_abbreviation",
        "bt_name",
        "dp",
        "alternative",
        "level",
    ]

    if not os.path.exists("temp"):
        os.makedirs("temp")

    with open(
        "temp/DigimonCards.csv", mode="w", newline="", encoding="utf-8"
    ) as csv_file:
        csv_writer = csv.writer(
            csv_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )

        csv_writer.writerow(headers)

        for page in bt_pages:
            url = page[0]
            bt_name = page[1]
            bt_abbreviation = page[2]

            response = requests.get(url)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "lxml")

                ul_element = soup.find("ul", class_="image_lists")

                if ul_element:
                    card_items = soup.find_all(
                        "li", class_=re.compile(r"image_lists_item data page-\d+")
                    )

                    for card_item in card_items:
                        alternative = card_item.find(
                            "li", class_="cardtype cardParallel"
                        )

                        popup_div = card_item.find("div", class_="popup")
                        first_div = popup_div.find("div")
                        colors = first_div.get("class")

                        img_div = card_item.find("img")
                        raw_image_url = img_div.get("src")
                        image_url = "https://world.digimoncard.com" + raw_image_url[2:]

                        card_head = card_item.find("ul", class_="cardinfo_head")
                        head_elements = [
                            element.text.strip() for element in card_head.find_all("li")
                        ]

                        rarity = head_elements[1]
                        card_type = head_elements[2]
                        name = card_item.find("div", class_="card_name").get_text()

                        # This part handles alternative art card versions
                        if alternative is None:
                            card_number = head_elements[0]
                            alternative = 0
                        else:
                            # Special case for alternative art get number from image URL
                            raw_card_number = raw_image_url.split("/")[-1]
                            raw_card_number = raw_card_number.split(".")[0]
                            card_number = raw_card_number
                            alternative = 1

                        dd_elements = card_item.find_all("dd")

                        colors = dd_elements[0].text.strip()
                        stage = dd_elements[1].text.strip()
                        attribute = dd_elements[2].text.strip()
                        types = dd_elements[3].text.strip()
                        dp = dd_elements[4].text.strip()
                        cost = dd_elements[5].text.strip()
                        effect = dd_elements[8].text.strip()
                        evolution_effect = dd_elements[9].text.strip()
                        security_effect = dd_elements[10].text.strip()
                        evolution_cost_one = dd_elements[6].text.strip()
                        evolution_cost_two = dd_elements[7].text.strip()

                        if types == "-":
                            types = "Null"
                            type_one = "Null"
                            type_two = "Null"
                        else:
                            if "/" in types:
                                type_one, type_two = types.split("/", 1)
                            else:
                                type_one = types
                                type_two = "Null"

                        if card_type == "Digimon":
                            if len(head_elements) >= 4:
                                level = head_elements[3]
                            else:
                                level = "Null"
                        else:
                            level = "Null"

                        if stage == "-":
                            stage = "Null"

                        if dp == "-":
                            dp = "Null"

                        if attribute == "-":
                            attribute = "Null"

                        if cost == "-":
                            cost = "Null"

                        if evolution_cost_one in ("-", ""):
                            evolution_cost_one = "Null"
                        else:
                            evolution_cost_one = re.search(
                                r"\d+", evolution_cost_one
                            ).group()

                        if evolution_cost_two in ("-", ""):
                            evolution_cost_two = "Null"
                        else:
                            evolution_cost_two = re.search(
                                r"\d+", evolution_cost_two
                            ).group()

                        if effect == "-":
                            effect = "Null"

                        if evolution_effect == "-":
                            evolution_effect = "Null"

                        if security_effect == "-":
                            security_effect = "Null"

                        color_list = colors.split()

                        color_one = color_list[0]
                        color_two = color_list[1] if len(color_list) > 1 else "NULL"
                        color_three = color_list[2] if len(color_list) > 2 else "NULL"

                        card = [
                            card_number,
                            name,
                            card_type,
                            rarity,
                            color_one,
                            color_two,
                            color_three,
                            image_url,
                            cost,
                            stage,
                            attribute,
                            type_one,
                            type_two,
                            evolution_cost_one,
                            evolution_cost_two,
                            effect,
                            evolution_effect,
                            security_effect,
                            bt_abbreviation,
                            bt_name,
                            dp,
                            alternative,
                            level,
                        ]

                        csv_writer.writerow(card)
                        print("Card written: " + card_number)
                else:
                    print("Could not find <ul> with class 'image_lists'.")
            else:
                print(f"Failed to retrieve page. Status code: {response.status_code}")

    _remove_csv_duplicates()


def create_db_structure():
    """
    Creates all tables and relationships in the database.
    """
    _create_database_if_not_exists()

    connection = _create_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS CardTypes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Rarities (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Colors (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Stages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Attributes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Types (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS BTs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            abbreviation VARCHAR(50) NOT NULL
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Cards (
            id INT AUTO_INCREMENT PRIMARY KEY,
            card_number VARCHAR(50) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            dp INT,
            card_type_id INT,
            rarity_id INT,
            color_one_id INT,
            color_two_id INT,
            color_three_id INT,
            image_url TEXT,
            cost INT,
            stage_id INT,
            attribute_id INT,
            type_one_id INT,
            type_two_id INT,
            evolution_cost_one INT,
            evolution_cost_two INT,
            effect TEXT,
            evolution_effect TEXT,
            security_effect TEXT,
            bt_id INT,
            alternative BOOLEAN CHECK (alternative IN (0,1)),
            FOREIGN KEY (card_type_id) REFERENCES CardTypes(id),
            FOREIGN KEY (rarity_id) REFERENCES Rarities(id),
            FOREIGN KEY (color_one_id) REFERENCES Colors(id),
            FOREIGN KEY (color_two_id) REFERENCES Colors(id),
            FOREIGN KEY (color_three_id) REFERENCES Colors(id),
            FOREIGN KEY (stage_id) REFERENCES Stages(id),
            FOREIGN KEY (attribute_id) REFERENCES Attributes(id),
            FOREIGN KEY (type_one_id) REFERENCES Types(id),
            FOREIGN KEY (type_two_id) REFERENCES Types(id),
            FOREIGN KEY (bt_id) REFERENCES BTs(id)
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Decks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            color_id INT,
            image TEXT,
            FOREIGN KEY (color_id) REFERENCES Colors(id)
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS DeckCards (
            card_number VARCHAR(50),
            deck_id INT,
            quantity INT NOT NULL,
            PRIMARY KEY (card_number, deck_id),
            FOREIGN KEY (card_number) REFERENCES Cards(card_number) ON DELETE CASCADE,
            FOREIGN KEY (deck_id) REFERENCES Decks(id) ON DELETE CASCADE
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Collection (
            card_number VARCHAR(50) PRIMARY KEY,
            quantity INT NOT NULL DEFAULT 1,
            FOREIGN KEY (card_number) REFERENCES Cards(card_number) ON DELETE CASCADE
        )"""
    )

    connection.commit()
    cursor.close()
    connection.close()

    print("Database structure created successfully")


def fill_db():
    """
    Stuff the database with card data from the CSV file.
    """
    
    connection = _create_connection()
    cursor = connection.cursor()

    card_types = {}
    rarities = {}
    colors = {}
    stages = {}
    attributes = {}
    types = {}
    bt_map = {}

    with open("temp/DigimonCards.csv", mode="r", encoding="utf-8") as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)

        for row in csv_reader:
            card_number = row[0]
            name = row[1]
            image_url = row[7]
            cost = row[8] if row[8].lower() != "null" else None
            evolution_cost_one = row[13] if row[13].lower() != "null" else None
            evolution_cost_two = row[14] if row[14].lower() != "null" else None
            effect = row[15] if row[15].lower() != "null" else None
            evolution_effect = row[16] if row[16].lower() != "null" else None
            security_effect = row[17] if row[17].lower() != "null" else None
            dp = row[20] if row[20].lower() != "null" else None
            alternative = int(row[21])

            card_type_id = _get_id(card_types, row[2], cursor, "CardTypes")
            rarity_id = _get_id(rarities, row[3], cursor, "Rarities")
            stage_id = (
                _get_id(stages, row[9], cursor, "Stages")
                if row[9].lower() != "null"
                else None
            )
            attribute_id = (
                _get_id(attributes, row[10], cursor, "Attributes")
                if row[10].lower() != "null"
                else None
            )
            type_one_id = _get_id(types, row[11], cursor, "Types")
            type_two_id = (
                _get_id(types, row[12], cursor, "Types")
                if row[12].lower() != "null"
                else None
            )

            color_one_id = _get_id(colors, row[4], cursor, "Colors")
            color_two_id = (
                _get_id(colors, row[5], cursor, "Colors")
                if row[5].lower() != "null"
                else None
            )
            color_three_id = (
                _get_id(colors, row[6], cursor, "Colors")
                if row[6].lower() != "null"
                else None
            )

            _insert_bt(cursor, row[18], row[19], bt_map)
            bt_id = bt_map[f"{row[18]}_{row[19]}"]

            cursor.execute(
                """
                INSERT INTO Cards (
                    card_number, name, dp, card_type_id, rarity_id, color_one_id, color_two_id, color_three_id,
                    image_url, cost, stage_id, attribute_id, type_one_id, type_two_id,
                    evolution_cost_one, evolution_cost_two, effect, evolution_effect, 
                    security_effect, bt_id, alternative
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    card_number,
                    name,
                    dp,
                    card_type_id,
                    rarity_id,
                    color_one_id,
                    color_two_id,
                    color_three_id,
                    image_url,
                    cost,
                    stage_id,
                    attribute_id,
                    type_one_id,
                    type_two_id,
                    evolution_cost_one,
                    evolution_cost_two,
                    effect,
                    evolution_effect,
                    security_effect,
                    bt_id,
                    alternative,
                ),
            )

    connection.commit()
    cursor.close()
    connection.close()
    print("Database populated successfully")


def download_images():
    """
    Snags card images from the web and converts them to tidy WebP format.
    """
    df = pd.read_csv("temp/DigimonCards.csv")

    if not os.path.exists("img"):
        os.makedirs("img")

    processed_count = 0
    max_workers = 10

    with ThreadPoolExecutor(max_workers=max_workers) as executor, tqdm(
        total=len(df)
    ) as progress:
        futures = []
        for _, row in df.iterrows():
            card_number = str(row["card_number"])
            image_url = row["image_url"]

            if pd.notna(image_url):
                if os.path.exists(f"img/{card_number}.webp"):
                    progress.update(1)
                    continue

                future = executor.submit(
                    _download_convert_image, image_url, card_number
                )
                future.add_done_callback(lambda _: progress.update(1))
                futures.append(future)

        for future in futures:
            future.result()
            processed_count += 1

    print(f"Processed {processed_count} images")


def update_db():
    """
    Freshens up the database with new cards.
    - Clears old CSV to avoid mixups
    - Only adds shiny new BTs you don't have yet
    """
    csv_path = "temp/DigimonCards.csv"
    if os.path.exists(csv_path):
        try:
            os.remove(csv_path)
            print("Removed existing CSV file to prevent duplicates")
        except Exception as e:
            print(f"Error removing CSV file: {str(e)}")
            return

    connection = _create_connection()
    if connection is None:
        print("Error de conexión. No se pudo actualizar.")
        return

    cursor = connection.cursor()
    existing_bts = _get_bts_from_server(cursor)
    cursor.close()
    connection.close()

    web_bts = _list_BTs()

    new_bts = [bt for bt in web_bts if (bt[2], bt[1]) not in existing_bts]

    if not new_bts:
        print("No hay nuevos BTs para actualizar")
        return

    create_csv(new_bts)

    fill_db()

    print("Base de datos actualizada correctamente")


def _create_connection():
    """
    Connecting to the database
    Returns:
        connection: MySQL hookup or None if something's broken
    """
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
        )
        return connection
    except Error as e:
        print(f"MySQL connection error: {e}")
        return None


def _create_database_if_not_exists():
    """
    Checks if the database exists and creates it if not.
    """
    db_name = os.getenv("DB_NAME")
    if not db_name:
        print("Error: DB_NAME is not defined in environment variables.")
        return

    try:
        # Connect without specifying database
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        cursor = connection.cursor()

        # Check database existence
        cursor.execute(
            "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s",
            (db_name,)
        )
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"Database '{db_name}' created successfully.")

        cursor.close()
        connection.close()

    except Error as e:
        print(f"Database creation error: {e}")


def _get_id(dictionary, value, cursor, table):
    """
    ID wrangler - either finds existing ID or creates new entry.

    Args:
        dictionary: Our cache to avoid duplicate lookups
        value: What we're looking up
        cursor: Database cursor
        table: Where to look/create

    Returns:
        int: The ID we need for this value
    """
    if value not in dictionary:
        cursor.execute(f"SELECT id FROM {table} WHERE name = %s", (value,))
        result = cursor.fetchone()
        if result:
            dictionary[value] = result[0]
        else:
            cursor.execute(f"INSERT INTO {table} (name) VALUES (%s)", (value,))
            dictionary[value] = cursor.lastrowid
    return dictionary[value]


def _insert_bt(cursor, abbreviation, name, bt_dict):
    key = f"{abbreviation}_{name}"
    if key not in bt_dict:
        cursor.execute(
            "SELECT id FROM BTs WHERE abbreviation = %s AND name = %s",
            (abbreviation, name),
        )
        result = cursor.fetchone()
        if result:
            bt_dict[key] = result[0]
        else:
            cursor.execute(
                "INSERT INTO BTs (abbreviation, name) VALUES (%s, %s)",
                (abbreviation, name),
            )
            bt_dict[key] = cursor.lastrowid


def _get_bts_from_server(cursor):
    """Get the BT lists we already have in the database."""
    cursor.execute("SELECT abbreviation, name FROM BTs")
    return cursor.fetchall()


def _list_BTs():
    """
    Website scraper - grabs the current list of BT sets available.
    Returns them in [url, name, abbreviation] format.
    """
    url = "https://world.digimoncard.com/cardlist"
    bt_list = []

    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        nav_list = soup.find("div", {"id": "snaviList"})
        links = nav_list.find_all("a")

        for link in links:
            title = link.find("span", {"class": "title"})
            href = "https://world.digimoncard.com/cardlist/" + link.get("href")
            title_text = title.get_text().strip()

            match = re.search(r"\[(.*?)\]", title_text)
            bt_abbreviation = match.group(1) if match else "P"

            bt_list.append([href, title_text, bt_abbreviation])

    else:
        print(f"Failed to retrieve BT list. Status code: {response.status_code}")

    return bt_list


def _remove_csv_duplicates():
    """Cleans up the CSV file - kicks out any duplicate card entries."""
    unique_entries = []

    with open("temp/DigimonCards.csv", "r+", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)
        csv_file.seek(0)
        csv_file.truncate()
        writer = csv.DictWriter(csv_file, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in rows:
            card_number = row["card_number"]
            if card_number not in unique_entries:
                unique_entries.append(card_number)
                writer.writerow(row)


def _download_convert_image(url, filename):
    """
    Downloads card art and converts to WebP.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()

        png_path = os.path.join("img", f"{filename}.png")
        with open(png_path, "wb") as f:
            f.write(response.content)

        img = Image.open(png_path).convert("RGBA")
        webp_path = os.path.join("img", f"{filename}.webp")
        img.save(webp_path, "WEBP")

        os.remove(png_path)
    except Exception as e:
        print(f"Error processing image {filename}: {str(e)}")


if __name__ == "__main__":
    # create_csv()
    create_db_structure()
    fill_db()
    # update_db()
    # download_images()
    print("All operations completed")
