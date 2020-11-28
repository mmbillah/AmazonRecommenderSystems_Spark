import json
import psycopg2
import math
import pandas as pd
import gzip

# function from https://nijianmo.github.io/amazon/index.html#code
# data set webpage detailing how to obtain data
def parse(path):
  g = gzip.open(path, 'rb')
  for l in g:
    yield json.loads(l)


# function from https://nijianmo.github.io/amazon/index.html#code
# data set webpage detailing how to obtain data
def getDF(path):
  i = 0
  df = {}
  for d in parse(path):
    df[i] = d
    i += 1
  return pd.DataFrame.from_dict(df, orient='index')


# This function is from the database class, credit to Dr. Sakire
def cleanStr4SQL(s):
  return s.replace("'", "`").replace("\n", " ")

def cleanStr4Int(s):
  return s.replace(",", "").replace("$", "")


def insert_reviews(df):
  try:
    conn = psycopg2.connect("dbname='group1' user='postgres'host='localhost' password='sophie'")
    print('Connected to local database')
  except:
    print('Unable to connect to the database!')
    exit()
  cur = conn.cursor()
  inserted = 0
  for items in df.iloc:
    rating = str(items[0])
    if (rating == 'nan'):
      rating = '0'
    verified = str(items[1])
    votes = cleanStr4Int(str(items[9]))
    if (votes == 'nan'):
      votes = '0'
    reviewTime = str(items[2])
    reviewerID = str(items[3])
    asin = str(items[4])
    reviewerName = cleanStr4SQL(str(items[5]))
    reviewText = cleanStr4SQL(str(items[6]))
    summary = cleanStr4SQL((str(items[7])))
    style = cleanStr4SQL((str(items[10])))


    sql_str = "INSERT INTO Reviews (rating, verified, votes, reviewTime, reviewerID, asin, " + \
              "reviewerName, reviewText, summary, style) VALUES ('" + rating + "', '" + verified + "', '" + \
              votes + "', '" + reviewTime + "', '" + reviewerID + "', '" + asin + "', '" + \
              reviewerName + "', '" + reviewText + "', '" + summary + "', '" + style + "');"

    try:
      cur.execute(sql_str)
    except:
      print("didn't work: " + sql_str)
      break
    conn.commit()
    inserted += 1
  return inserted



# There appears to be duplicates of item entries from the data set that is causing issues
def insert_metadata(df):
  try:
    conn = psycopg2.connect("dbname='group1' user='postgres'host='localhost' password='sophie'")
    print('Connected to local database')
  except:
    print('Unable to connect to the database!')
    exit()

  cur = conn.cursor()
  inserted = 0
  for items in df.iloc:
    asin = str(items['asin'])
    title = cleanStr4SQL(str(items[4]))
    feature = cleanStr4SQL(str(items[9]))
    if feature == '[]':
      feature = 'NA'
    description = cleanStr4SQL(str(items[2]))
    if description == '[]':
      description = 'NA'
    price = cleanStr4Int(str(items['price']))
    ## the 16th item has price as description or so it looks
    if price == 'nan' or price == '' or len(price) > 6:
      price = '0'
    salesRank = str(items[10])
    brand = cleanStr4SQL(str(items[8]))
    categories = str(items[0])
    if categories == '[]':
      categories = 'NA'

    sql_str = "INSERT INTO Item (asin, title, feature, description, price, salesRank, " + \
              "brand, categories) VALUES ('" + asin + "', '" + title + "', '" + \
              feature + "', '" + description + "', '" + price + "', '" + salesRank + "', '" + \
              brand + "', '" + categories + "');"

    try:
      cur.execute(sql_str)
    except:
      print("didn't work: " + sql_str)
      break
    conn.commit()

    if str(items['also_buy']) != '[]':
      for alsos in items['also_buy']:
        sql_str = "INSERT INTO AlsoBuy (asin, alsoBuy) VALUES ('" + asin + "', '" + str(alsos) + "');"
        try:
          cur.execute(sql_str)
        except:
          print("didn't work: " + sql_str)
          break
        conn.commit()

    if str(items['also_view']) != '[]':
      for alsos in items['also_view']:
        sql_str = "INSERT INTO AlsoView (asin, alsoView) VALUES ('" + asin + "', '" + str(alsos) + "');"
        try:
          cur.execute(sql_str)
        except:
          print("didn't work: " + sql_str)
          break
        conn.commit()

    inserted += 1
  return inserted
