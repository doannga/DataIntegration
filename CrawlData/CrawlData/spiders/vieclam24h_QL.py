# -*- coding: utf-8 -*-
import scrapy
from CrawlData.items import JobItem
import re



class Vieclam24hQlSpider(scrapy.Spider):
    name = 'vieclam24h_QL'
    start_urls = [
        'https://vieclam24h.vn/tim-kiem-viec-lam-nhanh/?hdn_nganh_nghe_cap1=&hdn_dia_diem=&hdn_tu_khoa=&hdn_hinh_thuc=&hdn_cap_bac=',
    ]

    def parse(self, response):
        
        for tn in response.xpath('//div[@class="list-items "]/div/div/span'):
            src = tn.xpath('a/@href').extract_first()
            src = response.urljoin(src)
            yield scrapy.Request(src, callback=self.parse_src)

        next_pages = response.xpath('//li[@class="next"]/a/@href').extract()
        next_page = next_pages[len(next_pages) - 1]
        
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)

    def parse_src(self, response):
        self.item = JobItem()
        self.item["url"] = response.request.url
        title = response.xpath('//div[@class="col-xs-12"]/h1[@class="text_blue font28 mb_10 mt_20 fws title_big"]/text()').extract()
        if len(title) > 0:
        	self.item["title"] = title[0]

        #Cong ty
        company = response.xpath('//p[@class="font16"]//a[@class="text_grey3"]/text()').extract()
        if len(company) > 0:
            self.item["company"] = company[0]
        #Luong
        salary = response.xpath('//div[@class="col-xs-6"]/p')
        if len(salary) > 0:
            salary = salary[0].xpath('span[@class="pl_28"]/span/text()').extract()
            if len(salary) > 0:
                self.item["salary"] = re.sub(r'<.*?>', '. ',salary[0])

        #Kinh nghiem    
        experience = response.xpath('((//div[@class="col-xs-6"])[1]//span[@class="pl_28"])[2]//span[@class="job_value"]/text()').extract()
        if len(experience) > 0:
            self.item["experience"] = experience[0]
            pass
         # Bang cap
        diploma = response.xpath('((//div[@class="col-xs-6"])[1]//span[@class="pl_28"])[3]//span[@class="job_value"]/text()').extract()
        if len(diploma) > 0:
            self.item["diploma"] = diploma[0]
            pass
         #So luong
        amount = response.xpath('((//div[@class="col-xs-6"])[1]//span[@class="pl_28"])[4]//span[@class="job_value"]/text()').extract()
        if len(amount) > 0:
            self.item["amount"] = amount[0]
            pass
         #Nganh nghe
        career = response.xpath('//h2[@class="pl_28 font14 fwb"]//a[@class="job_value text_pink"]/text()').extract() 
        if len(career) > 0:
            self.item["career"] = career[0]
            pass
         #Noi lam viec
        address = response.xpath('//span[@class="pl_28"]//a[@class="job_value text_pink"]/text()').extract()
        if len(address) > 0:
            self.item["address"] = address[0]
            pass
         # Chuc vu   
        position = response.xpath('((//div[@class="col-xs-6"])[2]//span[@class="pl_28"])[2]//span[@class="job_value"]/text()').extract()
        if len(position) > 0:
            self.item["position"] = position[0]
            pass

        #Hinh thuc lam viec fulltime/parttime
        category = response.xpath('((//div[@class="col-xs-6"])[2]//span[@class="pl_28"])[3]//span[@class="job_value"]/text()').extract()
        if len(category) > 0:
            self.item["category"] = category[0]
            pass
        #Thoi gian thu viec
        trial_time = response.xpath('((//div[@class="col-xs-6"])[2]//span[@class="pl_28"])[4]//span[@class="job_value"]/text()').extract()
        if len(trial_time) > 0:
            self.item["trial_time"] = trial_time[0]
            pass
        #Yeu cau gioi tinh
        sex = response.xpath('((//div[@class="col-xs-6"])[2]//span[@class="pl_28"])[5]//span[@class="job_value"]/text()').extract()
        if len(sex) > 0:
            self.item["sex"] = sex[0]
            pass
        #Yeu cau tuoi 
        age = response.xpath('((//div[@class="col-xs-6"])[2]//span[@class="pl_28"])[6]//span[@class="job_value"]/text()').extract() 
        if len(age) > 0:
            self.item["age"] = age[0]
            pass

        #Mo ta
        description =  response.xpath('(//div[@id="ttd_detail"]//div[@class="item row"])[1]//p[@class="col-md-9 pr_0 mb_0 word_break"]/text()').extract()
        if len(description) > 0:
            self.item["description"] = description[0]
            pass
        #Quyen loi duoc huong
        benefits = response.xpath('(//div[@id="ttd_detail"]//div[@class="item row"])[2]//p[@class="col-md-9 pr_0 mb_0 word_break"]/text()').extract()
        if len(benefits) > 0:
            self.item["benefits"] = benefits[0]
            pass
        #Yeu cau khac
        require_skill = response.xpath('(//div[@id="ttd_detail"]//div[@class="item row"])[3]//p[@class="col-md-9 pr_0 mb_0 word_break"]/text()').extract()
        if len(require_skill) > 0:
            self.item["require_skill"] = require_skill[0]
            pass
        #Thong tin lien he
        per_contact = response.xpath('(//div[@class="job_description bg_white pl_24 pr_24 mt_16 pb_18 box_shadow"]//div[@class="item row pt_14 pb_14"])[1]//p[@class="col-md-9 pr_0 mb_0"]').extract()
        add_contact = response.xpath('(//div[@class="job_description bg_white pl_24 pr_24 mt_16 pb_18 box_shadow"]//div[@class="item row pt_14 pb_14"])[2]//p[@class="col-md-9 pr_0 mb_0"]').extract()
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
        expired = response.xpath('(//span[@class="text_pink"])[1]/text()').extract() #Het han
        if len(expired) > 0:
            self.item["expired"] = expired[0][15:]
            pass
        #Ngay tao hoso
        created = response.xpath('(//p[@class="text_grey2 font12 mt8 mb12"]//span)[3]/text()').extract() #Ngay tao
        if len(created) > 0:
            created_at = created[0][14:]
            # replace("Ngày làm mới: ","")
            self.item["created"] = created_at
        if self.item["title"] != "":
            yield self.item
            

        

        	
