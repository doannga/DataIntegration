# -*- coding: utf-8 -*-
import scrapy
from CrawlData.items import JobItem
import re
import hashlib
import pybloom_live

class CareerbuilderSpider(scrapy.Spider):
    name = 'careerbuilder'
    start_urls = ['https://careerbuilder.vn/viec-lam/tat-ca-viec-lam-vi.html/']
    ur = pybloom_live.BloomFilter(capacity=2097152, error_rate=0.005)

    def parse(self, response):
        for tn in response.xpath('//h3[@class="job"]'):
            src = tn.xpath('a/@href').extract_first()
            src = response.urljoin(src)
            # self.ur
            add_url = self.ur.add(src)

            # self.box_urls[] = pybloom_live.BloomFilter(capacity=2097152, error_rate=0.005)
            # add_url = self.box_urls[].add(src)
            if add_url is False:
                yield scrapy.Request(src, callback=self.parse_src) 
                pass
            

        next_pages = response.xpath('//a[@class="right"]/@href').extract()
        next_page = next_pages[len(next_pages) - 1]
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)

    def parse_src(self, response):
    	self.item = JobItem()
        print(response.request.url)

        # if add_url is False:
        #     self.item["url"] = box_urls[0]

        self.item["url"] = response.request.url
        #Tieu de
     #    title = response.xpath('//div[@class="top-job-info"]/h1/text()').extract()
     #    print("Nga")
     #    title_ = re.sub('[s,.,?,!,W,#,@]', " ",title[0])
     #    title_items = title_.split(" ")
     #    m = hashlib.md5()
     #    for ite in title_items:
     #        m.update(ite)
     #    print(m.hexdigest())
     #    print("nga")
     #    if len(title) > 0:
     #        self.item["title"] = title[0]
     #    else:
     #        self.item["title"] = "" 
     #    # Ten cong ty
    	# company = response.xpath('//div[@class="tit_company"]/text()').extract()
    	# if len(company) > 0:
    	# 	self.item["company"] = company
    	# else:
    	# 	self.item["company"] = ""
    	# 	pass
    	# # Luong
    	# salary = response.xpath('(//p[@class="fl_right"])[2]//label/text()').extract()
    	# if len(salary) > 0:
    	# 	self.item["salary"] = salary
    	# else:
    	# 	self.item["salary"] = ""
    	# 	pass
    	# #Kinh nghiem    
     #    experience = response.xpath('(//p[@class="fl_left"])/text()').extract()
     #    if len(experience) > 0:
     #        self.item["experience"] = experience[0]
     #        pass
     #    # Bang cap
     #    diploma = response.xpath('(//div[@class="content_fck"]//ul//li)[1]/text()').extract()
     #    if len(diploma) > 0:
     #        self.item["diploma"] = diploma[0][14:]
     #        pass
     #     #So luong
     #    amount = ""
     #    self.item["amount"] = amount
     #     #Nganh nghe
     #    career = response.xpath('(//p[@class="fl_left"])[3]//b//a/text()').extract() 
     #    if len(career) > 0:
     #        self.item["career"] = career[0]
     #        pass
     #     #Noi lam viec
     #    address = response.xpath('(//p[@class="fl_left"])[1]//b//a/text()').extract()
     #    if len(address) > 0:
     #        self.item["address"] = address[0]
     #        pass
     #     # Chuc vu   
     #    position = response.xpath('(//p[@class="fl_right"])[1]//label/text()').extract()
     #    if len(position) > 0:
     #        self.item["position"] = position[0]
     #        pass

     #    #Hinh thuc lam viec fulltime/parttime
     #    category = response.xpath('(//div[@class="content_fck"]//ul//li)[3]/text()').extract()
     #    if len(category) > 0:
     #        self.item["category"] = category[0][14:]
     #        pass
     #    #Thoi gian thu viec
     #    trial_time = ""
     #    self.item["trial_time"] = trial_time
     #    #Yeu cau gioi tinh
     #    sex = response.xpath('(//div[@class="content_fck"]//ul)[1]//li/text()').extract()
     #    sex_strings = sex[0].split(" ")
     #    for sex_string in sex_strings:
     #        if sex_string == "nữ":
     #            self.item["sex"] = sex_string
     #            pass
     #        if sex_string == "nam":
     #            self.item["sex"] = sex_string
     #            pass
     #        if sex_string == "nam/nữ" or sex_string == "nữ/nam" or sex_string == "nam,nữ" :
     #            self.item["sex"] = sex_string
     #            pass
     #        else:
     #            sex_string = ""
     #            self.item["sex"] = sex_string
     #        pass
     #    age = response.xpath('(//div[@class="content_fck"]//ul//li)[2]/text()').extract() 
     #    if len(age) > 0:
     #        self.item["age"] = age[0][14:]
     #        pass

     #    #Mo ta
     #    description =  response.xpath('(//div[@class="content_fck"])[1]//p/text()').extract()
     #    if len(description) > 0:
     #        self.item["description"] = description[0]
     #        pass
     #    #Quyen loi duoc huong
     #    benefits = response.xpath('//ul[@class="list-benefits"]//li/text()').extract()
     #    if len(benefits) > 0:
     #        self.item["benefits"] = benefits[0]
     #        pass
     #    #Yeu cau khac
     #    require_skill = response.xpath('(//div[@class="content_fck"]//ul)[1]//li/text()').extract()
     #    if len(require_skill) > 0:
     #        self.item["require_skill"] = require_skill[0]
     #        pass
     #    #Thong tin lien he
     #    per_contact = response.xpath('(//p[@class="TitleDetailNew"]//label)[3]//strong/text()').extract()
     #    add_contact = response.xpath('(//p[@class="TitleDetailNew"]//label)[2]/text()').extract()
     #    if len(per_contact) > 0 :
     #        pers_contact = re.sub(r'<.*?>', ' ', per_contact[0])
     #        pers_contact = re.sub(r'\n', ' ', pers_contact)
     #        pers_contact = re.sub(r'\r', ' ', pers_contact)
     #        pass
     #    else:
     #        pers_contact = ""
     #    if len(add_contact) > 0:
     #        addr_contact = re.sub(r'<.*?>', ' ', add_contact[0])
     #        addr_contact = re.sub(r'\n', ' ', addr_contact)
     #        addr_contact = re.sub(r'\r', ' ', addr_contact)
     #        pass
     #    else:
     #        addr_contact = ""
     #    # contact = pers_contact + "\n" +addr_contact
     #    contact = u"Người liên hệ: " + pers_contact + u"Địa chỉ liên hệ: " + addr_contact
     #    self.item["contact"] = contact
        
     #    #Han nop ho so
     #    expired = response.xpath('(//p[@class="fl_right"])[3]/text()').extract() #Het han
     #    if len(expired) > 0:
     #        self.item["expired"] = expired[0]
     #        pass
     #    #Ngay tao hoso
     #    created = response.xpath('//div[@class="datepost"]//span/text()').extract() #Ngay tao
     #    if len(created) > 0:
     #        self.item["created"] = created[0]
     #    if self.item["title"] != "":
        yield self.item
    	
    def check_near_duplicate(self, url1, url2):
        m1 = re.search(r'\d+$', url1)
        m2 = re.search(r'\d+$', url2)
        if(m2.group() == '1'):
            return True
        elif(m1.group() == m2.group()):
            return True
        return False
