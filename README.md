# Digimon TCG Database Creator
This project is a Python script that automates the collection of data from the official **Digimon TCG** site to generate a complete database of cards.

## 🚀 What does this script do?
- It extracts all the cards from the **official Digimon TCG site**.
- Generates a `.csv` file with the collected data.
- Creates a database.
- Automatically fills the database with the card data.
- Download the images of the cards in **.webp** format.
- Allows to update the database when new charts are available, this is done by downloading the BTs that are not in the database.

---

## ⚙️ .env configuration
Before running the script, it is necessary to create an `.env` file in the root directory of the project with the following values:

```env
DB_HOST=your_host
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=your_database_name
```

# ⚠️ Warnings and limitations
-The data is obtained directly from the official Digimon TCG site, which may contain errors.
-For example, some cards do not have their rarity defined, which generates an entry with rarity “ ” (empty).
-This is not corrected by the script, as the Digimon website is being corrected, as of today 09/04/2025 the only error is the empty rarity but in the future there may be another or this error is no longer there.
-The only error that the script does correct automatically is when a card appears duplicated on the official website.

## 📌 License and Rights
This project is exclusively for personal use and non-commercial purposes.

All rights to the Digimon TCG card game belong to Bandai.
This project has no official relationship with Bandai or the game development team.

It only seeks to facilitate access to information for individual use.

## 🛠️ Coming soon
I'm working on:

An API built with FastAPI to interact with the database.

A web page to manage your collection, visualize cards and build your decks in an easy way.