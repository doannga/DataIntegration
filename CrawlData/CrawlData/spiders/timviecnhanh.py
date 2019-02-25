# -*- coding: utf-8 -*-
import scrapy
from CrawlData.items import JobItem
import re

class TimviecnhanhSpider(scrapy.Spider):
    name = 'timviecnhanh'
    start_urls = ['https://www.timviecnhanh.com/vieclam/timkiem?tu_khoa=&nganh_nghe%5B%5D=&tinh_thanh%5B%5D=']

    def parse(self, response):

        for tn in response.xpath('//td[@class="block-item w55"]'):
            src = tn.xpath('a/@href').extract_first()
            src = response.urljoin(src)
            yield scrapy.Request(src, callback=self.parse_src)


        next_pages = response.xpath('//a[@class="next item"]/@href').extract()
        next_page = next_pages[len(next_pages) - 1]

        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)

    def parse_src(self, response):
        self.item = JobItem()
        self.item["url"] = response.request.url
        title = response.xpath('//h1[@class="title font-roboto text-primary"]//span/text()').extract()
        if len(title) > 0:
            self.item["title"] = title[0]

        #Cong ty
        company = response.xpath('//div[@class="col-xs-6 p-r-10 offset10"]//h3//a/text()').extract()
        if len(company) > 0:
            self.item["company"] = company[0].strip()
        #Luong
        salary = response.xpath('((//li[@class="m-b-5"])/text())[2]')
        if len(salary) > 0:
        	self.item["salary"] = salary[0]

        #Kinh nghiem    
        experience = response.xpath('((//li[@class="m-b-5"])[2]/text())[2]').extract()
        if len(experience) > 0:
            self.item["experience"] = experience[0].strip()
            pass
        # Bang cap
        diploma = response.xpath('((//li[@class="m-b-5"])[3]/text())[2]').extract()
        if len(diploma) > 0:
            self.item["diploma"] = diploma[0].strip()
            pass
         #So luong
        amount = response.xpath('((//li[@class="m-b-5"])[6]/text())[2]').extract()
        if len(amount) > 0:
            self.item["amount"] = amount[0].strip()
            pass
         #Nganh nghe
        career = response.xpath('(//li[@class="m-b-5"])[5]//a/text()').extract() 
        if len(career) > 0:
            self.item["career"] = career[0]
            pass
         #Noi lam viec
        address = response.xpath('(//li[@class="m-b-5"])[4]//a/text()').extract()
        if len(address) > 0:
            self.item["address"] = address[0]
            pass
         # Chuc vu   
        # position = response.xpath('((//div[@class="col-xs-6"])[2]//span[@class="pl_28"])[2]//span[@class="job_value"]/text()').extract()
        # if len(position) > 0:
        #     self.item["position"] = position[0]
        #     pass

        #Hinh thuc lam viec fulltime/parttime
        category = response.xpath('((//li[@class="m-b-5"])[9]/text())[2]').extract()
        if len(category) > 0:
            self.item["category"] = category[0].strip()
            pass
        # #Thoi gian thu viec
        # trial_time = response.xpath('((//div[@class="col-xs-6"])[2]//span[@class="pl_28"])[4]//span[@class="job_value"]/text()').extract()
        # if len(trial_time) > 0:
        #     self.item["trial_time"] = trial_time[0]
        #     pass
        #Yeu cau gioi tinh
        # sex = response.xpath('((//li[@class="m-b-5"])[7]/text())[2]').extract()
        # if len(sex) > 0:
        #     self.item["sex"] = sex[0].strip()
        #     pass
        # #Yeu cau tuoi 
        # age = response.xpath('((//div[@class="col-xs-6"])[2]//span[@class="pl_28"])[6]//span[@class="job_value"]/text()').extract() 
        # if len(age) > 0:
        #     self.item["age"] = age[0]
        #     pass

        #Mo ta
        description =  response.xpath('(//article[@class="block-content"]//table//tr/td/p)[1]/text()').extract()
        if len(description) > 0:
            self.item["description"] = description[0]
            pass
        #Quyen loi duoc huong
        benefits = response.xpath('(//article[@class="block-content"]//table//tr/td/p)[3]/text()').extract()
        if len(benefits) > 0:
            self.item["benefits"] = benefits[0]
            pass
        #Yeu cau khac
        require_skill = response.xpath('(//article[@class="block-content"]//table//tr/td/p)[2]/text()').extract()
        if len(require_skill) > 0:
            self.item["require_skill"] = require_skill[0]
            pass
        #Thong tin lien he
        per_contact = response.xpath('(//table[@class="width-100"]//td//p)[1]/text()').extract()
        add_contact = response.xpath('(//table[@class="width-100"]//td//p)[2]/text()').extract()
        if len(per_contact) > 0 :
            pers_contact = re.sub(r'<.*?>', ' ', per_contact[0])
            pers_contact = re.sub(r'\n', ' ', pers_contact)
            pers_contact = re.sub(r'\r', ' ', pers_contact)
            pass
        else:
            pers_contact = ""
        if len(add_contact) > 0:
            addr_contact = re.sub(r'<.*?>', ' ', add_contact[0])
            addr_contact = re.sub(r'\n', ' ', addr_contact)
            addr_contact = re.sub(r'\r', ' ', addr_contact)
            pass
        else:
            addr_contact = ""
        # contact = pers_contact + "\n" +addr_contact
        contact = u"Người liên hệ: " + pers_contact + u"Địa chỉ liên hệ: " + addr_contact
        self.item["contact"] = contact
        
        #Han nop ho so
        expired = response.xpath('(//b[@class="text-danger"])[1]/text()').extract() #Het han
        if len(expired) > 0:
            self.item["expired"] = expired[0][15:]
            pass
        #Ngay tao hoso
        created = response.xpath('//time[@class="entry-date published"]/text()').extract() #Ngay tao
        if len(created) > 0:
            self.item["created"] = created[0]
        if self.item["title"] != "":
            yield self.item

