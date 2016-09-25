
# fetch them all!!!


from pydwork.pullin import *
from selenium import webdriver
from bs4 import BeautifulSoup
import os
import time
import re
import requests
import pandas as pd

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



dest_dir = "/Volumes/Seagate Backup Plus Drive/Seeking_Alpha/rawdata2/"


firefox_profile = webdriver.FirefoxProfile()

# firefox_profile.set_preference("webdriver.load.strategy", "unstable")

firefox_profile.add_extension("/Users/kenjin/pw/sa/adblock_plus-2.7.3-sm+tb+fx+an.xpi")
firefox_profile.add_extension("/Users/kenjin/pw/sa/quickjava-2.0.8-fx.xpi")
firefox_profile.set_preference("thatoneguydotnet.QuickJava.curVersion", "2.0.6.1") ## Prevents loading the 'thank you for installing screen'
firefox_profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.Images", 2)  ## Turns images off
firefox_profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.AnimatedImage", 2)  ## Turns animated images off

firefox_profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.CSS", 2)  ## CSS
# firefox_profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.Cookies", 2)  ## Cookies
firefox_profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.Flash", 2)  ## Flash
firefox_profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.Java", 2)  ## Java
# firefox_profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.JavaScript", 2)  ## JavaScript
firefox_profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.Silverlight", 2)  ## Silverlight


# Getting article addresses is easy
# just use 'requests'
# remember to pass headers={'User-Agent': 'Mozilla/5.0'}
# so seekingalpha doesn't block you

def get_links(start_page, end_page):
    addr_base = "http://seekingalpha.com/articles?page="

    article_links = []
    for i in range(start_page, end_page):
        addr = addr_base + str(i)
        result = []
        while result == []:
            try:
                # seeking alpha is picky
                page = requests.get(addr, headers={'User-Agent': 'Mozilla/5.0'}).text
                soup = BeautifulSoup(page, "html.parser")
                box = soup.select("ul.stripes_list")
                links = box[0].select("li > div > a")
                if len(links) != 75:
                    raise Exception
                else:
                    print("succeeded fetching links", addr)
                    for link in links:
                        result.append(link['href'])
            except:
                print("failed, taking a rest for 3 secs")
                time.sleep(3)
                pass
        for r in result:
            article_links.append(r)
    return article_links



def fetch_article_pages(addrs):
    def init_driver(email, password):
        driver = webdriver.Firefox(firefox_profile=firefox_profile)

        driver.implicitly_wait(10)
        driver.get("http://seekingalpha.com/account/login")
        time.sleep(3)
        driver.find_element_by_css_selector("#login_user_email").send_keys(email)
        driver.find_element_by_css_selector("#login_user_password").send_keys(password)
        driver.find_element_by_css_selector("#orthodox_login .register_button").click()
        time.sleep(5)
        return driver


    def fetchfn(driver, item):
        base_addr = "http://seekingalpha.com"
        driver.get(base_addr + item)
        # wait(driver, ".sa-comment-block")

        page = driver.page_source
        # try:
        #     soup = BeautifulSoup(page, "html.parser")
        #     soup.select("#a-hd")[0]
        #     soup.select("#a-cont")[0]
        # except:
        #     raise Exception
        # else:
        #     return pd.DataFrame({'address': [item],
        #     'page': [page]
        #     })

        return pd.DataFrame({'address': [item],
        'page': [page]
        })

    driver1 = init_driver("jinisrolling@gmail.com", "ahfoslwo")
    # driver2 = init_driver("aakenjin@gmail.com", "ahfoslwo")

    fetch_items([driver1], addrs, fetchfn, max_trials=10, base_dir=dest_dir, max_items=500)


def make_some_samples():
    result_file = False
    for fn in os.listdir(dest_dir):
        if fn.startswith("result"):
            result_file = fn
            break
    df = pd.read_csv(os.path.join(dest_dir, result_file))
    count = 0
    for r in df.iterrows():
        with open(os.path.join(dest_dir, r[1].address[9:-1] + ".html"), "w") as f:
            if isinstance(r[1].page, str):
                f.write(r[1].page)
            else:
                f.write('malformed')
        count += 1


if __name__ == '__main__':
    # with open("links_2014.csv", "w") as f:
    #     for link in get_links(1400, 2040):
    #         f.write(link + "\n")

    links = []
    with open("links_2014.csv") as f:
        for link in f:
            links.append(link)
    fetch_article_pages(links)
