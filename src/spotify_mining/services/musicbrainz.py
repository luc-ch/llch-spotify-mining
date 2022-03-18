import requests
import re
import json
from datetime import datetime as dt
from random import randint
from time import sleep


basePath = "https://musicbrainz.org/ws/2/"
date_format = "%Y-%m-%d"
date_format_alternative = "%Y"


def __get_release(query='artist:"Eminem" AND artist:"Rihanna"'):
    path = basePath + "release"
    payload = {"version": 2, "fmt": "json", "query": query, "limit": 5, "offset": 0}

    r = requests.get(path, payload)
    if not r.ok:
        sleep(randint(20, 100) / 10)
        print(r)
        return __get_release(query)
    j = json.loads(r.text)
    return j


def get_first_release(releases):
    first_release = releases[0]
    try:
        min_date = dt.now()
        for p in releases:
            if "date" not in p:
                print("DATE SMELLS!!!!!!!!!!!!")
                continue
            try:
                date = dt.strptime(p["date"], date_format)
            except ValueError:
                date = dt.strptime(p["date"], date_format_alternative)
            if date < min_date:
                first_release = p
    except Exception:
        pass
    return first_release


def get_last_release(releases):
    last_release = releases[0]
    try:
        max_date = dt.strptime("2017-01-01", date_format)
    except ValueError:
        max_date = dt.strptime("2017-01-01", date_format_alternative)
    for p in releases:
        if "date" not in p:
            print("DATE SMELLS!!!!!!!!!!!!")
            continue
        try:
            date = dt.strptime(p["date"], date_format)
        except ValueError:
            date = dt.strptime(p["date"], date_format_alternative)
        if date > max_date:
            last_release = p
    print(f"lastRelease {last_release}")
    return last_release


def __get_single_release_loop(name, artist):
    name = re.sub("[\(\[].*?[\)\]]", "", name).split("-", 1)[0].strip()
    name_query = f'releaseaccent:"{name}"'
    try:
        artist_query = " AND ".join(
            [f"artist:\"{art['artist_name']}\"" for art in artist]
        )
        query = f"{name_query} AND {artist_query}"
        print(query)
        print(f"query", end=" ")
        json = __get_release(query)
        print(f"{name} - json['count'] {json['count']}")
        if json["count"] == 0:
            if len(artist) == 1:
                if name == "":
                    print(f"Track not in musicbrainz 3.")
                    print(f"[{artist_query}] ", end="")
                    return None, name
                name = ""
            else:
                artist = artist[:-1]
            json, name = __get_single_release_loop(name, artist)
            if json is None:
                return None, name
        return json, name
    except FloatingPointError:
        print("FloatingPointError")
        print(f"{name} [{artist}]")
        return None, name


def get_single_release(data_in):
    name = data_in["track_name"]
    artists = data_in["artists"]

    json, crop_name = __get_single_release_loop(name, artists)
    if not json:
        return None
    if "releases" not in json:
        print("releases SMELLS!!!!!!!!!!!!")
        print(json)
    if crop_name == "":
        release = get_last_release(json["releases"])
    else:
        release = get_first_release(json["releases"])
    if release is not None:
        release["track_id"] = data_in["track_id"]
    return release


def get_label_by_id(id="f669a7a4-b94b-42db-a0b7-ba040e5c30d8"):
    path = f"https://musicbrainz.org/ws/2/label/{id}"
    payload = {"fmt": "json", "inc": "label-rels", "limit": 5, "offset": 0}
    r = requests.get(path, payload)
    if not r.ok:
        sleep(randint(20, 100) / 10)
        return get_label_by_id(id)
    j = json.loads(r.text)
    a = []
    for ix, elem in enumerate(j["relations"]):
        if elem["label"]["type"] == "Holding":
            owner = {}
            owner["id"] = elem["label"]["id"]
            owner["name"] = elem["label"]["name"]
            owner["data"] = elem["label"]
            a.append(owner)
    if a != []:
        j["owner"] = a
    del j["relations"]
    return j
