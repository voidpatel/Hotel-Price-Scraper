import csv
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP
from weakref import ProxyTypes

import mysql.connector
import requests
from pandas import DataFrame
from pretty_html_table import build_table
from selectorlib import Extractor
from selectorlib.formatter import Formatter

# establishing connection with AWS RDS(mysql)
con = mysql.connector.connect(
    host="AWS RDS database Endpoint",
    user="your username",
    password="your password",
    database="database name",
    port=3306
)

# check if python-RDS connection is successfull
if con:
    print("connected")

# creating a cursor to run sql queries in python
cur = con.cursor()

# fetching data(name and price column) from database
cur.execute("select name,price from unique_prices")
rows = list(cur.fetchall())
# converting list to dictionary to compare with the new fetched prices
d_olddata = {x[0]: x[1] for x in rows}

# class to replace 'US$' to empty string and return integer to compare it with updated values


class Price(Formatter):
    def format(self, text):
        price = text.replace('US$', '').strip()
        return int(price)


# Create an Extractor by reading from the YAML file
e = Extractor.from_yaml_file(
    '/Users/patelneel/Documents/Hotel_Price_Scaper/booking (1).yml', formatters=[Price])


def scrape(url):
    headers = {
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'DNT': '1',
        'Upgrade-Insecure-Requests': '1',
        # You may want to change the user agent if you get blocked
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',

        'Referer': 'https://www.booking.com/index.en-gb.html',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
    }

# Download the page using requests
#    print("Downloading %s" % url)
    r = requests.get(url, headers=headers)
# Pass the HTML of the page and create
    return e.extract(r.text, base_url=url)


now = datetime.datetime.today()  # today's day month and year
# timetravelling one day ahead with timedelta function
buffer_time = datetime.timedelta(1)
tomorrow = now + buffer_time  # adding one day to today in order to get tomorrow

current_day = now.day
current_month = now.month
current_year = now.year

next_day = tomorrow.day
next_month = tomorrow.month
next_year = tomorrow.year

# generating customised url for each day
URL = 'https://www.booking.com/searchresults.html?aid=337776&sid=9c96d7c121fc5dfa55d95134f0388076&tmpl=searchresults&ac_click_type=b&ac_position=0&checkin_month=' + str(current_month) + '&checkin_monthday=' + str(current_day) + '&checkin_year=' + str(current_year) + '&checkout_month=' + str(next_month) + '&checkout_monthday=' + str(next_day) + '&checkout_year=' + str(
    next_year) + '&city=20011460&class_interval=1&dest_id=20011460&dest_type=city&dtdisc=0&from_sf=1&group_adults=2&group_children=0&iata=BFL&inac=0&index_postcard=0&label_click=undef&no_rooms=1&order=price&postcard=0&raw_dest_type=city&room1=A%2CA&sb_price_type=total&search_selected=1&shw_aparth=1&slp_r_match=0&srpvid=e4731db596c20069&ss=Bakersfield%2C%20California%2C%20United%20States&ss_all=0&ss_raw=baker&ssb=empty&sshis=0&ssne=Bakersfield&ssne_untouched=Bakersfield&top_ufis=1&nflt=class%3D2%3B&rsf='

# saving new data (name, price) in local csv file to compare it with the old data
with open('data.csv', 'w') as outfile:
    fieldnames = [
        "name",
        "price"
    ]
    writer = csv.DictWriter(
        outfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    # for url in URL.readlines():
    data = scrape(URL)
    if data:
        for h in data['hotels']:
            writer.writerow(h)

with open('data.csv') as c:
    reader = csv.reader(c, delimiter=',')
    data_list = list(reader)

# converting list to dictionary
d_newdata = {x[0]: x[1] for x in data_list[1:]}
# converting str datatype to integer for price column
new_d = d_newdata.items()
new_d = {key: int(value) for key, value in new_d}

# old and new price comparison function


def updated_price():
    result = []
    for key in d_olddata:
        if key in new_d:
            if d_olddata[key] != new_d[key]:
                old_price = d_olddata[key]
                new_price = new_d[key]
                # appending new prices if change in price
                result.append((key, old_price, new_price))
            elif d_olddata[key] == new_d[key]:
                old_price = d_olddata[key]
                # appending null value if the price has not changed
                result.append((key, old_price, '-'))
    return result


latest_value = updated_price()

latest_price = []
for items in latest_value:
    latest_price.append(items[2])

print(latest_price)

# constructing a dataframe for prettyhtml table function
df = DataFrame(latest_value, columns=[
               'Motel Name', 'Old Price', 'New Price'])

messageHTML = '<p> Stay Updated!!!<ol><li> Source: BOOKING.COM </li><li> Daily email notifications are scheduled at 11am, 4pm, and 8pm</li><li> Checkin Date: Today || Checkout Date: Tomorrow </li></ol></p>'

if latest_price.count('-') < len(latest_price):
    def send_mail(body):

        # using MIME to send html data
        message = MIMEMultipart()
        message['Subject'] = 'ALERT!!!: Motel prices around you has changed!'
        message['From'] = 'yourusername@gmail.com'   #input your gmail id
        message['To'] = 'yourusername@gmail.com'     #input your own gmail or any business gmail id

        body_content = body
        message.attach(MIMEText(messageHTML, "html"))
        message.attach(MIMEText(body_content, "html"))
        msg_body = message.as_string()

        server = SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(message['From'], 'yourpassword')
        server.sendmail(message['From'], message['To'], msg_body)
        server.quit()

    def send_email():
        # conversion of dataframe to html table
        output = build_table(df, 'blue_light')
        send_mail(output)
        print("Mail sent successfully.")

    send_email()

else:
    print("No updated prices")

# delete old prices from rds database and insert new prices
cur.execute("DELETE from unique_prices")
cur.executemany(
    "REPLACE INTO unique_prices(name, price) VALUES(%s, %s)", data_list[1:])

# commiting all your sql queries and closing the cursor connection
con.commit()
cur.close()
con.close()
