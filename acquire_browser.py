from selenium import webdriver
from time import sleep
from re import sub

login = '33677622162'
passwd = 'Mmsr960514spvk'

def _wait(b, element_name, by='class', click=False, timeout=7):

    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    try:
        if by == 'class':
            element_present = EC.presence_of_element_located((By.CLASS_NAME, element_name))
            WebDriverWait(b, timeout).until(element_present)

        elif by == 'id':
            element_present = EC.presence_of_element_located((By.ID, element_name))
            WebDriverWait(b, timeout).until(element_present)
        return True
    except:
        return False


def setup():
    b = webdriver.Chrome()
    return b

def authenticate(b):
    url = 'https://vk.com'
    b.get(url)
    b.execute_script("window.submitQuickLoginForm(\"{0}\", \"{1}\")".format(login, passwd))
    return b

def main(query):
    b = setup()
    b = authenticate(b)

    search_url = url = 'https://vk.com/search?c[likes]=10&c[per_page]=40&c[q]={}&c[section]=statuses&c[type]=1'.format(query)
    b.get(search_url)
    _wait(b, 'page_block_header_count')
    count = int(sub(' ', '', b.find_element_by_class_name('page_block_header_count').text))
    scrolls = count // 40 + 1
    for i in scrolls:
        b.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
    html = b.page_source
    with open('{}.html'.format(search_url.split('/')[1]), 'w') as f:
        f.write(html)

if __name__ == '__main__':
    query = 'сталин'
    main(query)