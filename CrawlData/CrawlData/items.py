# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class JobItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    cluster = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()  #Tieu de
    company = scrapy.Field()
    salary=scrapy.Field()
    experience = scrapy.Field()
    diploma = scrapy.Field()
    amount = scrapy.Field()  #So luong
    career = scrapy.Field()   #Nganh nghe
    address = scrapy.Field()
    position = scrapy.Field()       # Chuc vu
    category = scrapy.Field()  # fulltime-partime
    trial_time = scrapy.Field()
    sex = scrapy.Field()
    age = scrapy.Field()
    description = scrapy.Field()   #mo ta
    benefits = scrapy.Field()
    require_skill = scrapy.Field()  #yeu cau khac
    contact = scrapy.Field()      
    expired = scrapy.Field()
    created = scrapy.Field()
    pass
