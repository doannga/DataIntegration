# -*- coding: utf-8 -*-
import scrapy
# from itviec.items import ItviecItem
import re

class Vieclam24hSpider(scrapy.Spider):
    name = 'vieclam24h'
    allowed_domains = ['vieclam24h.vn']
    start_urls = ['https://vieclam24h.vn']
    url_list=[]

    def start_requests(self):
        urls_dict={}
        urls_dict[5]='https://vieclam24h.vn/ajax/box-xacthuc-nganh.html?container=box_vieclam_xacthuc&box_id=269%2C273%2C274%2C275&nganh_nghe=5&limit=10&pk_tin_tuyen_dung=2978658%2C2983809%2C2978902%2C2976609%2C2984384%2C2981328%2C2462558&tinh_id=2%2C22%2C26%2C27%2C32%2C42%2C43%2C45%2C48%2C49%2C50%2C54%2C56%2C57%2C58%2C59%2C61%2C63%2C64%2C65%2C68%2C69%2C70%2C74%2C76%2C77%2C78%2C81%2C85%2C91%2C92%2C94%2C95%2C96%2C97%2C99&page='
        urls_dict[74]='https://vieclam24h.vn/ajax/box-xacthuc-nganh.html?container=box_vieclam_xacthuc&box_id=269%2C273%2C274%2C275&nganh_nghe=74&limit=10&pk_tin_tuyen_dung=2983604%2C2982626%2C2980962%2C2980961%2C2978117%2C2976859%2C2749957%2C2979913%2C2977824%2C2977674%2C2977545%2C2214587&tinh_id=2%2C22%2C26%2C27%2C32%2C42%2C43%2C45%2C48%2C49%2C50%2C54%2C56%2C57%2C58%2C59%2C61%2C63%2C64%2C65%2C68%2C69%2C70%2C74%2C76%2C77%2C78%2C81%2C85%2C91%2C92%2C94%2C95%2C96%2C97%2C99&page='
        for i in range(1,25):
            url_5=urls_dict[5]+str(i)
            yield scrapy.Request(url=url_5, callback=self.parse)
        for i in range(1,35):
            url_74=urls_dict[74]+str(i)
            yield scrapy.Request(url=url_74, callback=self.parse)

    def parse(self, response):
        # print(response)
        urls=response.selector.xpath('//a[contains(@class,\'text_grey2\')]/@href').extract()
        # print(url)
        for url in urls:
            print(url)
            # self.url_list.append(url)
            curl=self.start_urls[0]+url
            yield scrapy.Request(url=curl, callback=self.parse_detail)
        # pass

    def parse_detail(self,response):

        des=response.selector.xpath('//div[contains(@class,\'info-left\') or contains(@id,\'ttd_detail\')]/descendant::*/text()').extract()
        des_str=''
        for i in des:
            temp=re.sub(' +',' ',i)
            temp=re.sub('[\n\t\r]',' ',temp)
            if(len(temp)>1):
                des_str+=temp
        