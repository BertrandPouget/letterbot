import requests
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
import numpy as np
from tqdm import tqdm
tqdm.pandas()

import json
import re
import numpy as np
import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_metadata(url):
    # 1) GET e parsing base
    try:
        r = requests.get(url)
        r.raise_for_status()
    except:
        # se la request fallisce restituisci tutto in errore
        return pd.Series({
            "Plot": "SCRAPE ERROR",
            "Duration": "SCRAPE ERROR",
            "Director1": "SCRAPE ERROR",
            "Director2": "SCRAPE ERROR",
            "Genre1": "SCRAPE ERROR",
            "Genre2": "SCRAPE ERROR",
            "Genre3": "SCRAPE ERROR",
            "Country1": "SCRAPE ERROR",
            "Country2": "SCRAPE ERROR",
            "Country3": "SCRAPE ERROR",
            "Year": "SCRAPE ERROR",
            "RatingAvg": "SCRAPE ERROR",
            "RatingCount": "SCRAPE ERROR"
        })
    soup = BeautifulSoup(r.text, "html.parser")

    # 2) trama
    try:
        plot = soup.find("meta", {"name": "description"})["content"].strip()
    except:
        plot = "SCRAPE ERROR"

    # 3) durata da footer
    duration = np.nan
    footer = soup.find("p", class_="text-link text-footer")
    if footer:
        txt = footer.get_text()
        m = re.search(r'(\d+)\s*min', txt)
        if m:
            duration = int(m.group(1))

    # default error‑values per JSON‑LD
    defaults = {
        "directors": ["SCRAPE ERROR","SCRAPE ERROR"],
        "genres":    ["SCRAPE ERROR","SCRAPE ERROR","SCRAPE ERROR"],
        "countries": ["SCRAPE ERROR","SCRAPE ERROR","SCRAPE ERROR"],
        "year":      "SCRAPE ERROR",
        "rating_avg":"SCRAPE ERROR",
        "rating_count":"SCRAPE ERROR"
    }

    # 4) JSON‑LD
    tag = soup.find("script", type="application/ld+json")
    if not tag:
        directors, genres, countries = defaults["directors"], defaults["genres"], defaults["countries"]
        year, rating_avg, rating_count = defaults["year"], defaults["rating_avg"], defaults["rating_count"]
    else:
        try:
            js = json.loads(re.search(r'\{.*\}', tag.get_text(), re.DOTALL).group(0))

            # registi
            d = js.get("director", [])
            if not isinstance(d, list): d = [d]
            directors = [x.get("name","N/D") for x in d][:2]
            directors += [np.nan] * (2 - len(directors))

            # generi
            g = js.get("genre", [])
            if not isinstance(g, list): g = [g]
            genres = g[:3] + [np.nan]*(3-len(g))

            # anno
            reld = js.get("releasedEvent", [])
            year = (reld[0].get("startDate") if reld and isinstance(reld,list) else "N/D")

            # paesi
            c = js.get("countryOfOrigin", [])
            if not isinstance(c, list): c = [c]
            countries = [x.get("name","N/D") for x in c][:3]
            countries += [np.nan] * (3 - len(countries))

            # rating
            agg = js.get("aggregateRating", {})
            rating_avg   = agg.get("ratingValue", np.nan)
            rating_count = agg.get("ratingCount", np.nan)

        except:
            directors, genres, countries = defaults["directors"], defaults["genres"], defaults["countries"]
            year, rating_avg, rating_count = defaults["year"], defaults["rating_avg"], defaults["rating_count"]

    # 5) ritorna le colonne
    return pd.Series({
        "Plot": plot,
        "Duration": duration,
        "Director1": directors[0],
        "Director2": directors[1],
        "Genre1": genres[0],
        "Genre2": genres[1],
        "Genre3": genres[2],
        "Country1": countries[0],
        "Country2": countries[1],
        "Country3": countries[2],
        "Year": year,
        "RatingAvg": rating_avg,
        "RatingCount": rating_count
    })


if __name__ == "__main__":
    
    print("Loading data...")
    ratings = pd.read_csv("data/ratings.csv")
    watchlist = pd.read_csv("data/watchlist.csv")

    print("Enriching ratings...")
    new_cols = ratings['Letterboxd URI'].progress_apply(scrape_metadata)
    ratings_enriched = pd.concat([ratings, new_cols], axis=1)
    ratings_enriched.to_csv("data/ratings_enriched.csv", index=False)

    print("Enriching watchlist...")
    new_cols = watchlist['Letterboxd URI'].progress_apply(scrape_metadata)
    watchlist_enriched = pd.concat([watchlist, new_cols], axis=1)
    watchlist_enriched.to_csv("data/watchlist_enriched.csv", index=False)
