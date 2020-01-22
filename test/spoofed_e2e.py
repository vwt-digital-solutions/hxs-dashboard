import time
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import TimeoutException, WebDriverException
import traceback

try:
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--shm-size=2g')
    options.add_argument("--remote-debugging-port=9222")
    options.headless = True
    driver = webdriver.Chrome('/usr/bin/chromedriver', options=options)
except FileNotFoundError:
    print('Chromedriver not found, exiting test')
    sys.exit(1)
except WebDriverException as e:
    print('The webdriver threw an exception, exiting test')
    print(str(e))
    traceback.print_exc()
    sys.exit(1)
except OSError as e:
    print('OS threw an error (You\'re probably trying to run on a wrong OS), exiting test')
    print(str(e))
    sys.exit(1)
try:
    identifier = sys.argv[1]
except IndexError:
    print('No identifier provided, exiting test')
    sys.exit(1)
try:
    token = sys.argv[2]
except IndexError:
    print('No token provided, exiting test')
    sys.exit(1)

has_failing = False


def it_should(text, func):
    print('\033[95m' + "it should:" + '\033[0m', text)
    if func == "Passed":
        print('\033[92m        ' + func + '\033[0m')
    else:
        print('\033[91m        ' + func + '\033[0m')
        global has_failing
        has_failing = True


def page_load():
    try:
        WebDriverWait(driver, 30).until(
            ec.presence_of_element_located((By.ID, "react-entry-point"))
        )
    except TimeoutException:
        return "Failed: driver took too long (Something unexpected must have happened)"
    except Exception as e:
        return "Failed: " + str(e)
    return "Passed"


def problems_page():
    try:
        WebDriverWait(driver, 30).until(
            ec.presence_of_element_located((By.ID, "problem_count"))
        )
    except TimeoutException:
        return "Failed: driver took too long (Something unexpected must have happened)"
    except Exception as e:
        return "Failed: " + str(e)
    return "Passed"


def problems_table():
    try:
        for i in range(0, 5):
            if (len(driver.find_elements_by_xpath("//table[@tabindex='-1']/tbody/tr")) - 10) > 0:
                return "Passed"
            else:
                time.sleep(1)
        return "Failed: No problems found in table"
    except Exception as e:
        return "Failed: " + str(e)


def test():
    driver.get("https://" + identifier + ".appspot.com/?access_token=" + token)
    it_should('authenticate', page_load())
    driver.get("https://" + identifier + ".appspot.com/apps/datamanagement_problems/")
    it_should('open the problems page', problems_page())
    it_should('display more than 0 problems in table', problems_table())
    sys.exit(1) if has_failing else sys.exit(0)


test()
