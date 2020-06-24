import re
import socket
import requests
import requests.packages.urllib3.util.connection as urllib3_cn
import json
from bs4 import BeautifulSoup as bs
from time import sleep
from multiprocessing.dummy import Pool as ThreadPool
from itertools import chain
import nltk


def allowed_gai_family():
    return socket.AF_INET  # force ipv4


head = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0'}
base = 'https://www.imsdb.com'
classes = ['0'] + [chr(i) for i in range(65, 91)]


def get_urls_one_class(c):
    print('Parsing Class: ' + c)
    urls = []
    entry = requests.get(base + '/alphabetical/' + c, headers=head)
    entry.encoding = entry.apparent_encoding
    soup = bs(entry.text, "html.parser")
    tags = soup.find_all('p')
    hrefs = [base + tag.find_all('a')[0].get('href') for tag in tags]
    for href in hrefs:
        entry = requests.get(href, headers=head)
        entry.encoding = entry.apparent_encoding
        tags = bs(entry.text, "html.parser").find(class_='script-details').find_all('a')[-1].get('href')
        print(tags)
        if 'scripts' in tags:
            urls.append(base + tags)
    return urls


def get_urls():
    print(classes)
    pool = ThreadPool()
    urls_list = pool.map(get_urls_one_class, classes)
    pool.close()
    pool.join()
    urls_class = {}
    for i, c in enumerate(classes):
        urls_class[c] = urls_list[i]
    print(urls_class)
    with open('urls.json', 'w', encoding='utf-8') as f:
        json.dump(urls_class, f)
    return urls_class


def get_text_in_page(url):
    entry = requests.get(url, headers=head)
    entry.encoding = entry.apparent_encoding
    return entry.text


def get_dialog_in_text(text, name):
    lines = [line.replace('\t', ' ') for line in text.split('\n') if len(line.strip()) > 0]
    string = []
    last_char = ""
    length = len(lines)
    i = 0
    while i < length:
        start = i + 1
        if (lines[i].strip().startswith('<b') or lines[i].strip().startswith('</b><b')) and is_character_name(lines[i]):
            character = clean_html(lines[i]).strip()
            dialog = ""
            while start < length:
                d = lines[start].strip()
                if (d.strip()[:2] == '<b'):
                    break
                d = d.strip('<br>').strip('/b>').strip()
                if len(d) < 2 or len(d) > 45:
                    break
                dialog += d + ' '
                start += 1
            char = character.strip()
            dial = re.sub(r' +|:', ' ', dialog)
            dial = re.sub(r"\(.*?\)|<.*?>|\[.*?\]|--", '', dial).replace('...', '').strip()
            if len(char) > 1 and len(dial) > 1 and (last_char != char):
                string.append(char + ": " + ' '.join(nltk.word_tokenize(dial)))
            last_char = char
        i = start
    # print('\n'.join(string))
    print(f'Get Dialog: {len(string)} in url: {name}')
    return string


urls_class = {}


def get_dialogs(c):
    dialogs = [get_dialog_in_text(get_text_in_page(url), url) for url in urls_class[c]]
    dialogs = list(chain(*dialogs))
    with open(f'./dialog_{c}.txt', 'w', encoding='utf-8') as f:
        for dialog in dialogs:
            f.write(dialog + '\n')
    return dialogs


def get_page():
    global urls_class
    with open('urls.json', 'r', encoding='utf-8') as f:
        urls_class = json.load(f)
    pool = ThreadPool(12)
    pool.map(get_dialogs, urls_class.keys())
    pool.close()
    pool.join()


def clean_html(line):
    text = bs(line.replace("</b><b", "<b")).text
    text = re.sub(r'\*|\(|\)|--', ' ', text)
    return text


def is_character_name(line):
    temp = clean_html(line).strip('<br>').strip('/b>').strip()
    if temp.isupper():
        if re.search('INT.|EXT.|:|-|/|!', temp) != None or temp.endswith("."):
            return False
        if len(temp.split(' ')) > 2:
            return False
        return True
    else:
        return False


def clean_data_file(c):
    dialogs = []
    with open(f'dialog_{c}.txt', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        length = len(lines)
        i = 0
        while i < length-1:
            if '?' in lines[i]:
                pair = (lines[i].split(':')[-1].strip(), lines[i+1].split(':')[-1].strip())
                dialogs.append(pair)
            i += 1
    return dialogs


def clean_data_files(lower=False):
    pool = ThreadPool(12)
    dialogs = pool.map(clean_data_file, classes)
    pool.close()
    pool.join()
    dialogs = list(chain(*dialogs))
    name = 'lower' if lower else 'nolower'
    # save
    with open(f'dialog_data_{name}.txt', 'w', encoding='utf-8') as f:
        for dialog in dialogs:
            line = dialog[0] + '|' + dialog[1] + '\n'
            f.write(line.lower() if lower else line)


if __name__ == '__main__':
    '''
    urllib3_cn.allowed_gai_family = allowed_gai_family

    test_url = 'https://www.imsdb.com/scripts/Joker.html'
    get_dialog_in_text(get_text_in_page(test_url), test_url)  # for test

    get_urls()
    get_page()

    clean_data_files(lower=True)
    clean_data_files(lower=False)
    '''
    from cotk.dataloader import OpenSubtitles
    

