import requests
import urllib.parse
from bs4 import BeautifulSoup,element
import pymysql

# /連接SQL
db = pymysql.connect(host="",
                     user="",
                     passwd="",
                     db="",
                     charset="")
cur = db.cursor()
# 連接SQL/

# 爬 看版文張列表
def doc(INDEX):
    web = requests.get(INDEX)
    soup = BeautifulSoup(web.text, 'lxml')  # 默認情況下，Beautiful Soup將文檔解析為HTML。要將文檔解析為XML
    articles = soup.find_all('div', 'r-ent')  # 文章列表皆為div > r-ent
    NOT_EXIST = BeautifulSoup('<a>本文已被刪除</a>', 'lxml').a

    all_posts = []
    for article in articles:
        meta = article.find('div', 'title').find('a') or "None"
        if "None" not in meta:
            posts = {
                'title': meta.getText().strip(),
                'link': 'https://www.ptt.cc%s' % meta.get('href'),
                'push': article.find('div', 'nrec').getText(),
                'author': article.find('div', 'author').getText(),
            }
            all_posts.append(posts)

    return all_posts


def reply(title, link, push, author):
    reply_web = requests.get(link)
    reply_soup = BeautifulSoup(reply_web.text, 'lxml')  # 默認情況下，Beautiful Soup將文檔解析為HTML。要將文檔解析為XML

    content = reply_soup.find(id="main-content")
    body2 = list(filter(lambda x: type(x) == element.NavigableString, list(content.children)))

    body = ''
    body = body.join(body2)
    date = list(list(content.children)[3])[1].get_text()

    tags = reply_soup.select("div.push > span.push-tag")
    reply_users = reply_soup.select("div.push > span.push-userid")
    push_contents = reply_soup.select("div.push > span.push-content")
    push_ipdatetimes = reply_soup.select("div.push > span.push-ipdatetime")

    ret = []
    for tag, user, content, datetime in zip(tags, reply_users, push_contents, push_ipdatetimes):
        data = {
            'tag': tag.get_text(),
            'user': user.get_text(),
            'content': content.get_text(),
            'datetime': datetime.get_text().strip(),
            'title': title,
            'link': link,
            'push': push,
            'date': date,
            'author': author,
            'body': body
        }
        ret.append(data)
    return ret


INDEX = 'https://www.ptt.cc/bbs/Soft_Job/index.html'
INDEX2 = []
pages = int(input('請輸入載入幾頁:'))
page_url = INDEX
for i in range(pages):
    # 控制頁面選項: 最舊/上頁/下頁/最新
    web = requests.get(page_url)
    soup = BeautifulSoup(web.text, 'lxml')  # 默認情況下，Beautiful Soup將文檔解析為HTML。要將文檔解析為XML
    btn = soup.find('div', 'btn-group btn-group-paging').find_all('a', 'btn')
    link = btn[1].get('href')
    page_url = urllib.parse.urljoin(INDEX, link)
    INDEX2.append(page_url)
print(INDEX2)
d1 = doc(INDEX)
doc_id = 1
reply_id = 1
taglist = {'徵才': 1, '情報': 2, '新聞': 3, '請益': 4, '心得': 5, '討論': 6, '公告': 7, '板務': 8, '問卷': 9, '其他': 99, }
ret = []
for i in d1:
    r = reply(**i)
    ret.append(r)


for x in ret:
    for y in x:
        '''
        回文推: 'tag' : tag.get_text(),
        回文作者: 'user': user.get_text(),
        回文: 'content': content.get_text(),
        回文時間: 'datetime': datetime.get_text().strip(),
        主旨: 'title': title,
        網址: 'link': link,
        推文數: 'push': push,
        文章時間: 'date': date,
        文章作者: 'author': author,
        文章內容: 'body': body
        '''
        reply_sql = "INSERT INTO reply(reply_id, doc_id, palindrome, tag, reply_author, reply_time) VALUES (%s, %s, %s, %s, %s,%s )"
        reply_param = (reply_id, doc_id, y['content'], y['tag'], y['user'], y['datetime'])
        reply_n = cur.execute(reply_sql, reply_param)
        db.commit()
        reply_id += 1
    # 尋找主旨前綴並放入變數tag_id
    if y['title'].find(']') > 0:
        prepare_title = (y['title'][(y['title'].find('[')) + 1:(y['title'].find(']'))])
        tag_id = (taglist.get(prepare_title, '99'))
    else:
        prepare_title = '%@'
        tag_id = (taglist.get(prepare_title, '0'))

    sql = "INSERT INTO doc(doc_id,tag_id,doc_author,title, tweets, url,doc_time,article) VALUES (%s, %s, %s, %s, %s,%s ,%s,%s )"
    param = (doc_id, tag_id, y['author'], y['title'], y['push'], y['link'], y['date'], y['body'])
    n = cur.execute(sql, param)
    db.commit()
    doc_id += 1
    reply_id = 1
db.close()
