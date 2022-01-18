import itertools
from multiprocessing import Pool
from datetime import datetime
import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import requests
import pymongo

import re

from pymongo import UpdateOne, MongoClient
from pymongo.errors import BulkWriteError

all_categories = list()
f = open('kategorier.json')

data = json.load(f)

for i in data:
    all_categories.append(i['category'])

f.close()

base_url = 'https://www.finn.no/job/fulltime/search.html?'

all_urls = list()
myclient = pymongo.MongoClient("mongodb://something:something@something:27017/")
mydb = myclient["jobs"]
mycol = mydb["finn"]
client = MongoClient("mongodb://something:something@something:27017/')


def generate_urls():
    for a in all_categories:
        a = a.replace('-', '=')
        all_urls.append(base_url + str(a) + '&page=')


def generate_adurls():
    lista = list()
    filter = {}
    project = {
        'link': 1
    }

    result = mycol.find(
        filter=filter,
        projection=project
    )
    for test in result:
        # if bool(re.findall(r"\bwww.finn.no\b", test['link'])) == False:
        #     print('falsk')
        
        # if bool(re.search(test['link'], "\bwww.finn.no\b")) != True:
        #     test['link'] = 'www.finn.no'
        #     print(test['link'], 'not matching')
            
            
        lista.append(test['link'])

    return lista


def scrape(url):
    try:
        listofobjects = list()
        global link

        nextButtonExists = True
        pageincrement = 1
        while nextButtonExists:
            res = requests.get(url + str(pageincrement))
            res = res.text
            soup = BeautifulSoup(res, 'html.parser')
            a = soup.find_all("article", class_="ads__unit")
            for i2 in a:
                href1 = i2.findAll("a", class_="ads__unit__link", href=True)
                for link12 in href1:
                    if link12['href']:
                        if 'https://www.finn.no' not in link12['href']:
                            link12 = 'https://www.finn.no' + link12['href']
                            link = link12
                        else:
                            link = link12['href']
                findeasyapply = i2.find("span", class_="status status--success u-mb8")
                easyapply = False
                if findeasyapply is not None:
                    for text in findeasyapply:
                        if text.get_text() == 'Enkel søknad':
                            easyapply = True
                        else:
                            easyapply = False
                adtitle = i2.find("a",
                                class_="ads__unit__link").get_text()
                searc = i2.findAll("div", class_="ads__unit__content__list")
                company = None
                nbrofpositions = None
                if len(searc) == 1:
                    nbrofpositions = int(searc[0]
                                        .get_text().replace('stillinger', '').replace('stilling', ''))
                if len(searc) == 2:
                    company = i2.findAll("div", class_="ads__unit__content__list")[0].get_text()
                    nbrofpositions = int(i2.findAll("div", class_="ads__unit__content__list")[1]
                                        .get_text().replace('stillinger', '').replace('stilling', ''))
                href = i2.find("a", class_="ads__unit__link", href=True)
                href = href['href']
                jobid = re.findall(r"[0-9]+", href)[0]
                ds = {'_id': jobid, 'numberofpositions': nbrofpositions, 'easyapply': easyapply, 'adtitle': adtitle,
                    'company': company, 'link': link}
                listofobjects.append(ds)

            buttonExists = soup.find('span', class_='icon icon--chevron-right')
            if buttonExists is None:
                nextButtonExists = False

            pageincrement += 1
        return listofobjects
    except:
        pass

def get_dl(soup):
    keys, values = [], []

    for dl in soup.findAll("dl", {"class": "definition-list definition-list--inline"}):
        for dt in dl.findAll("dt"):
            keys.append(dt.text.strip())
        for dd in dl.findAll("dd"):
            values.append(dd.text.strip())
    return dict(zip(keys, values))


def adscrape(url):
    listofobjects = list()
    try:
        res = requests.get(url)
        res = res.text
        soup = BeautifulSoup(res, 'html.parser')
        #
        arr = []
        keys, values1 = [], []

        for dl in soup.findAll("dl", class_="definition-list definition-list--inline"):
            for dt in dl.findAll("dt"):
                keys.append(dt.text.strip())
                key = dt.text.strip()

                values = []
                for dd in dt.nextSiblingGenerator():
                    if dd.name == "dt":
                        break
                    if dd.name == "dd":
                        values.append(dd.text.strip())
                values1.append(values)

            arr.append(values1)
        data = dict(zip(keys, values1))

        asd = soup.select('body > main > div > div.grid > div.grid__unit.u-r-size1of3 > section:nth-child(2)')
        keys, values, contactData = [], [], []
        for dl in soup.select('body > main > div > div.grid > div.grid__unit.u-r-size1of3 > section:nth-child(2)'):
            for sd in dl.findAll("div", {"data-controller": "showPhoneNumber"}):
                for dt in sd.findAll("dt"):
                    keys.append(dt.text.strip())
                for dd in dl.findAll("dd"):
                    values.append(dd.text.strip())
                contactData.append(dict(zip(keys, values)))
        data['Kontakt'] = contactData
        keys, values1 = [], []

        for dl in soup.select(
                "body > main > div > div.grid > div.grid__unit.u-r-size2of3 > div > section:nth-child(3) > dl"):
            for dt in dl.findAll("dt"):
                keys.append(dt.text.strip())
                key = dt.text.strip()

                values = []
                for dd in dt.nextSiblingGenerator():
                    if dd.name == "dt":
                        break
                    if dd.name == "dd":
                        values.append(dd.text.strip())
                values1.append(values)

            arr.append(values1)
        data12 = dict(zip(keys, values1))

        for asd in data12:
            data[asd] = data12[asd]

        for key in data:
            listOfStrings = ['Sted', 'Flere arbeidssteder', 'Sektor', 'Antall stillinger', 'Arbeidsgiver', 'Frist',
                            'Hjemmekontor',
                            'Ansettelsesform', 'Stillingstittel']
            if key in listOfStrings:
                data[key] = data[key][0]
                if key == 'Antall stillinger':
                    data[key] = int(data[key])
            if key == 'Bransje' or key == 'Stillingsfunksjon':
                litt = list()
                for dat in data[key]:
                    # remove comma in end
                    dat = re.sub(r',*$', '', dat)
                    litt.append(dat)
                data[key] = litt
        asdd = soup.select(
            'body > main > div > div.grid > div.grid__unit.u-r-size2of3 > div > div.import-decoration > section')
        for text in asdd:
            text = {
                'html': text.decode_contents(),
                'formatted': text.get_text()
            }
            data['description'] = text
        applybutton = soup.select('body > main > div > div.grid > div.grid__unit.u-r-size1of3 > section:nth-child(3)')
        for link in applybutton:

            link = link.find('a', class_="button button--cta u-size1of1")
            try:
                if link.has_attr('href') is not None:
                    data['applylink'] = link['href']
            except:
                pass
        data['link'] = url
        if 'Antall stillinger' not in data:
            data['Antall stillinger'] = 1
        listofobjects.append(data)

        # print(data)
        #     valueContact.append(values)
        #
        # print(keyContact,valueContact)
        return listofobjects
    except:
        pass

generate_urls()

if __name__ == "__main__":
    p = Pool(32)
    total_successes = p.map(scrape, all_urls)  # Returns a list of lists
    p.terminate()
    p.join()
    te = [ent for sublist in total_successes for ent in sublist]
    uniques = pd.DataFrame(te).drop_duplicates().to_dict('records')
    for i in uniques:
        mycol.update_one({'_id': i['_id']}, {"$set": i}, upsert=True)
    links = generate_adurls()

    asd = Pool(32)
    total_ads = asd.map(adscrape, links)
    asd.terminate()
    asd.join()
    te1 = [ent for sublist in total_ads for ent in sublist]

    for i in te1:
        mycol.update_one({'link': i['link']}, {"$set": i}, upsert=True)

	
    now = datetime.now()
    print(now)
