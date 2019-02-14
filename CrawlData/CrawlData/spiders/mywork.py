# -*- coding: utf-8 -*-
import scrapy
from CrawlData.items import JobItem
import re

class MyworkSpider(scrapy.Spider):
    name = 'mywork'
    start_urls = ['https://mywork.com.vn/tuyen-dung/']

    def parse(self, response):
        for tn in response.xpath('//div[@class="row job-item"]/div/div/div/p'):
            src = tn.xpath('a/@href').extract_first()
            src = response.urljoin(src)
            yield scrapy.Request(src, callback=self.parse_src)

        next_pages = response.xpath('//a[@class="page-link" and ./span[contains(text(),"Trang sau")]]/@href').extract()
        next_page = next_pages[len(next_pages)-1]
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)
        pass
    def parse_src(self, response):
    	self.item = JobItem()
        self.item["url"] = response.request.url
        title = response.xpath('//h1[@class="main-title"]//span/text()').extract()
        if len(title) > 0:
        	self.item["title"] = title[0]

        #Cong ty
        company = response.xpath('(//h2[@class="desc-for-title mb-15"]//span)[1]/text()').extract()
        if len(company) > 0:
            self.item["company"] = company[0]
        #Luong
        salary = response.xpath('//div[@class="col-xs-6"]/p')
        if len(salary) > 0:
            salary = salary[0].xpath('//span[@class="text_red"]/text()').extract()
            if len(salary) > 0:
                self.item["salary"] = re.sub(r'<.*?>', '. ',salary[0])

        #Kinh nghiem    
        experience = response.xpath('(//div[@class="item item1"]//p)[1]/text()').extract()
        if len(experience) > 0:
            self.item["experience"] = experience[0]
            pass
         # Bang cap
        diploma = response.xpath('(//div[@class="item item1"]//p)[2]/text()').extract()
        if len(diploma) > 0:
            self.item["diploma"] = diploma[0]
            pass
         #So luong
        amount = response.xpath('(//div[@class="item item1"]//p)[3]/text()').extract()
        if len(amount) > 0:
            self.item["amount"] = amount[0]
            pass
         #Nganh nghe
        career = response.xpath('//span[@class="el-tag el-tag--primary"]//a/text()').extract() 
        if len(career) > 0:
            self.item["career"] = career[0].strip()
            pass
         #Noi lam viec
        address = response.xpath('//span[@class="el-tag location_tag el-tag--primary"]//a/text()').extract()
        if len(address) > 0:
            self.item["address"] = address[0].strip()
            pass
         # Chuc vu   
        position = response.xpath('(//div[@class="item item2"]//p)[2]/text()').extract()
        if len(position) > 0:
            self.item["position"] = position[0]
            pass

        #Hinh thuc lam viec fulltime/parttime
        category = response.xpath('(//div[@class="item item2"]//p)[1]/text()').extract()
        if len(category) > 0:
            self.item["category"] = category[0]
            pass
        #Thoi gian thu viec
        trial_time = ""
        self.item["trial_time"] = trial_time
        #Yeu cau gioi tinh
        sex = response.xpath('(//div[@class="item item2"]//p)[3]/text()').extract()
        if len(sex) > 0:
            self.item["sex"] = sex[0]
            pass
        #Yeu cau tuoi 
        age = ""
        self.item["age"] = age

        #Mo ta
        description =  response.xpath('(//div[@class="mw-box-item"])[1]/text()').extract()
        if len(description) > 0:
            self.item["description"] = description[0]
            pass
        #Quyen loi duoc huong
        benefits = response.xpath('(//div[@class="mw-box-item"])[2]/text()').extract()
        if len(benefits) > 0:
            self.item["benefits"] = benefits[0]
            pass
        #Yeu cau khac
        require_skill1 = response.xpath('(//div[@class="mw-box-item"])[3]/text()').extract()
        require_skill2 = response.xpath('(//div[@class="mw-box-item"])[4]/text()').extract()
        if len(require_skill1) > 0:
            require_skill11 = require_skill1[0]
            pass
        else:
        	require_skill11 = ""
        if len(require_skill2) > 0:
        	require_skill22 = require_skill2[0]
        	pass
        else:
        	require_skill22 = ""
        self.item["require_skill"] = require_skill11 + "\n" +require_skill22
        #Thong tin lien he
        per_contact = response.xpath('(//div[@class="col-md-6 col-lg-9 item"]//span)[1]/text()').extract()
        add_contact = response.xpath('(//div[@class="col-md-6 col-lg-9 item"]//span)[2]/text()').extract()
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
        contact = u"Người liên hệ: " + pers_contact + u"\n" +u"Địa chỉ liên hệ: " + addr_contact
        self.item["contact"] = contact
        
        #Han nop ho so
        expired = response.xpath('//div[@class="col-md-7"]//span[@class=""]/text()').extract() #Het han
        if len(expired) > 0:
            self.item["expired"] = expired[0]
            pass
        #Ngay tao hoso
        created = response.xpath('(//div[@class="content text-center pt-30"]//p)[2]/text()').extract() #Ngay tao
        if len(created) > 0:
            self.item["created"] = created[0]
        if self.item["title"] != "":
            yield self.item
    	pass
