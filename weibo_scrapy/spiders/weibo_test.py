# -*- coding: utf-8 -*-
from scrapy.http import Request,FormRequest
from scrapy.spiders import CrawlSpider
# from urllib.parse import quote_plus
from urllib import pathname2url
from scrapy.selector import Selector
from weibo_scrapy.items import InformationItem, TweetsItem, FollowsItem, FansItem
from PIL import Image
import base64
import time
import rsa
import binascii
import requests
import re

class weibo_spider(CrawlSpider):
    name = 'weibo_test'
    session = requests.session()
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
    headers = {
    'User-Agent': user_agent
                }

    def start_requests(self):
        start_url = 'http://weibo.com/login.php'
        return[Request(start_url,meta={'cookiejar':1},callback=self.get_su)]

    def get_su(self,response):
        # username = input('auto@kffund.cn')
        username = 'auto@kffund.cn'
        username_quote = pathname2url(username)
        uername_base64 = base64.b64encode(username_quote.encode('utf-8'))
        uername_base64 = uername_base64.decode('utf-8')
        print(uername_base64)
        #print(response.url)
        meta = {'cookiejar':response.meta['cookiejar'],
                'uername_base64':uername_base64}
        pre_url = "http://login.sina.com.cn/sso/prelogin.php?entry=weibo&callback=sinaSSOController.preloginCallBack&su="\
        + uername_base64 + "&rsakt=mod&checkpin=1&client=ssologin.js(v1.4.18)&_=" + str(int(time.time() * 1000))
        return[Request(pre_url,meta=meta,callback=self.server_data)]

    def server_data(self,response):
        #eval的作用是把list,tuple,dict和string相互转化
        server_data = eval(response.body.decode('utf-8').replace("sinaSSOController.preloginCallBack",''))
        print(server_data)
        meta =response.meta
        meta['server_data'] = server_data
        return[Request('https://baidu.com',meta=meta,callback=self.get_password)]

    def get_password(self,response):
        password = 'Kffund123'
        servertime = response.meta['server_data']['servertime']
        nonce = response.meta['server_data']['nonce']
        pubkey = response.meta['server_data']['pubkey']
        rsapublickey = int(pubkey,16)   #转话为10进制
        key = rsa.PublicKey(rsapublickey,65537)
        message = str(servertime) + '\t' + str(nonce) + '\n' + str(password)
        message = message.encode("utf-8")
        passwd = rsa.encrypt(message, key)
        passwd = binascii.b2a_hex(passwd)
        print(passwd)
        meta =response.meta
        meta['passwd'] = passwd
        return[Request('http://www.sina.com.cn/',meta=meta,callback=self.login)]

    def login(self,response):
        su = response.meta['uername_base64']
        servertime = response.meta['server_data']['servertime']
        nonce = response.meta['server_data']['nonce']
        rsakv = response.meta['server_data']['rsakv']
        password_secret = response.meta['passwd']
        postdata = {
        'entry':'weibo',
        'gateway':'1',
        'from':'',
        'savestate':'7',
        'useticket':'1',
        'pagerefer':"http://login.sina.com.cn/sso/logout.php?entry=miniblog&r=http%3A%2F%2Fweibo.com%2Flogout.php%3Fbackurl",
        'wsseretry':'servertime_error',
        'vsnf':'1',
        'su':su,
        'service':'miniblog',
        'servertime':str(servertime),
        'nonce':nonce,
        'pwencode':'rsa2',
        'rsakv':rsakv,
        'sp':password_secret,
        'sr':'1536*864',
        'encoding':'UTF-8',
        'prelt':'105',
        'url':'http://weibo.com/ajaxlogin.php?framelogin=1&callback=parent.sinaSSOController.feedBackUrlCallBack',
        'returntype':'META'
        }
        login_url = 'http://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.18)'
        print(postdata)
        if response.meta['server_data']['showpin'] == 0:
            return[FormRequest(login_url,meta={'cookiejar':response.meta['cookiejar']},formdata=postdata,callback=self.get_loop_url,dont_filter = True)]
        """
        else:   #获得验证吗，手动输入
            pcid = response.meta['server_data']['picd']
            cha_url = "http://login.sina.com.cn/cgi/pin.php?r="
            cha_url = cha_url + str(int(random.random() * 100000000)) + "&s=0&p="
            cha_url = cha_url + pcid
            cha_page = session.get(cha_url,cookies=response.meta['cookiejar'],headers=headers)
            with open("cha.jpg", 'wb') as f:
                f.write(cha_page.content)
                f.close()
            try:
                im = Image.open("cha.jpg")
                im.show()
                im.close()
            except:
                print(u"请到当前目录下，找到验证码后输入")
        """

    def get_loop_url(self,response):
        login_loop = response.body.decode('gbk')
        pa = r'location\.replace\([\'"](.*?)[\'"]\)'
        loop_url = re.findall(pa, login_loop)[0]
        print(loop_url)
        return[Request(loop_url,meta={'cookiejar':response.meta['cookiejar']},callback=self.after_login)]

    def after_login(self,response):
        return[Request('http://weibo.com/',meta={'cookiejar':response.meta['cookiejar']},callback=self.parse)]
        # return[Request('http://weibo.cn/3607000911/profile?filter=1&page=1',meta={'cookiejar':response.meta['cookiejar']},callback=self.parse2)]

    def parse(self, response):
        print 'parse-->' + response.url
        ID = '3607000911'
        # url_tweets = "http://weibo.cn/%s/profile?filter=1&page=1" % ID
        url_tweets = "https://weibo.com/u/%s?refer_flag=0000015010_&from=feed&loc=nickname&is_all=1#1521681877710" % ID
        yield Request(url=url_tweets, meta={"ID": ID}, callback=self.parse2)  # 去爬微博



    def parse2(self, response):
        """ 抓取微博数据 """
        print 'parse2-->' + response.url
        selector = Selector(response)
        # tweets = selector.xpath('body/div[@class="c" and @id]')
        tweets = selector.xpath('body/div[@class="c"]')
        for tweet in tweets:
            tweetsItems = TweetsItem()
            id = tweet.xpath('@id').extract_first()  # 微博ID
            content = tweet.xpath('div/span[@class="ctt"]/text()').extract_first()  # 微博内容
            cooridinates = tweet.xpath('div/a/@href').extract_first()  # 定位坐标
            like = re.findall(u'\u8d5e\[(\d+)\]', tweet.extract())  # 点赞数
            transfer = re.findall(u'\u8f6c\u53d1\[(\d+)\]', tweet.extract())  # 转载数
            comment = re.findall(u'\u8bc4\u8bba\[(\d+)\]', tweet.extract())  # 评论数
            others = tweet.xpath('div/span[@class="ct"]/text()').extract_first()  # 求时间和使用工具（手机或平台）

            tweetsItems["ID"] = response.meta["ID"]
            tweetsItems["_id"] = response.meta["ID"] + "-" + id
            if content:
                tweetsItems["Content"] = content.strip(u"[\u4f4d\u7f6e]")  # 去掉最后的"[位置]"
            if cooridinates:
                cooridinates = re.findall('center=([\d|.|,]+)', cooridinates)
                if cooridinates:
                    tweetsItems["Co_oridinates"] = cooridinates[0]
            if like:
                tweetsItems["Like"] = int(like[0])
            if transfer:
                tweetsItems["Transfer"] = int(transfer[0])
            if comment:
                tweetsItems["Comment"] = int(comment[0])
            if others:
                others = others.split(u"\u6765\u81ea")
                tweetsItems["PubTime"] = others[0]
                if len(others) == 2:
                    tweetsItems["Tools"] = others[1]
            yield tweetsItems
        url_next = selector.xpath(
            u'body/div[@class="pa" and @id="pagelist"]/form/div/a[text()="\u4e0b\u9875"]/@href').extract()
        if url_next:
            yield Request(url=self.host + url_next[0], meta={"ID": response.meta["ID"]}, callback=self.parse2)