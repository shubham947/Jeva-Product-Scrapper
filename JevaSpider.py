# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
import os
import csv
import glob
import pymysql
import logging

logging.basicConfig(filename='spiderLog.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filemode='w')

class JevaspiderSpider(scrapy.Spider):
    name = 'JevaSpider'
    allowed_domains = ['nykaa.com']
    start_urls = ['https://www.nykaa.com/brands/jeva/c/4321?sourcepage=home&rtqu=Jeva&eq=desktop&root=search,brand_menu,brand_list,Jeva']

    logging.info('Spider Started...')
    nextPage = 2

    def check_list(self, temp):
        if len(temp) == 0:
            return ''
        else:
            return temp[1]

    def parse(self, response):
        try:
            product_urls = response.xpath('//*[@class="product-list-box card desktop-cart"]/a/@href').extract()
            logging.info('Scraping page ' + str(self.nextPage - 1) + ' products')
            for product in product_urls:
                absolute_url = 'https://www.nykaa.com' + product
                yield Request(absolute_url, callback=self.parse_product)
        except Exception:
            logging.warning('Something went wrong.', exc_info=True)

        num = len(response.xpath('//*[@class="pagination-box"]/li').extract()) - 1 - self.nextPage
        if num:
            logging.info('Got next page url')
            try:
                next_page_url = response.xpath('//*[@class="next"]/a/@href').extract_first()
                absolute_next_page_url = 'https://www.nykaa.com' + next_page_url[0:-1] + str(self.nextPage)
                self.nextPage += 1
                yield Request(absolute_next_page_url)
            except Exception:
                logging.warning('Something went wrong.', exc_info=True)

    def parse_product(self, response):
        name = response.xpath('//*[@class="product-title"]/text()').extract_first()
        quantity = self.check_list(response.xpath('//*[@class="product-title"]/span/text()').extract())
        avg_rating = response.xpath('//*[@itemprop="ratingValue"]/@content').extract_first()
        total_review = response.xpath('//*[@itemprop="reviewCount"]/@content').extract_first()
        number_of_ratings = response.xpath('//*[@itemprop="ratingCount"]/@content').extract_first()
        mrp = self.check_list(response.xpath('//*[@class="product-des__details-price"]//span[@class="mrp-price"]/text()').extract())
        our_price = self.check_list(response.xpath('//*[@class="product-des__details-price"]//span[@class="post-card__content-price-offer"]/text()').extract())
        image_url = response.xpath('//*[@class="post-card__img-magnifier"]//img/@src').extract_first()
        # image_alt = response.xpath('//*[@class="post-card__img-magnifier"]//img/@alt').extract_first()
        description = response.xpath('//*[@class="Aplus-container"]/text()').extract_first()

        slug = response.request.url.split("/")[3]

        category = 'skincare'
        if "wax" in name.lower():
            category = 'waxing'

        yield {
            'name': name,
            'slug': slug,
            'image_url': image_url,
            'image_alt': '',
            'short_description': description,
            'category': category,
            'MRP': mrp,
            'our_price': our_price,
            'product_url': response.request.url,
            'number_of_ratings': number_of_ratings,
            'avg_rating': avg_rating,
            'total_review': total_review,
            'quantity': quantity
        }

    def close(self, reason):
        table_name = 'table_name'
        csv_file = max(glob.iglob('*.csv'), key=os.path.getctime)

        try:
            logging.info('Connecting to Database...')
            connection = pymysql.connect(host='sqlHostIP',
                                         user='username',
                                         password='password',
                                         db='DBname')

            with connection:
                logging.info('Connected to Database.')
                cur = connection.cursor()
                logging.info('Reading data from csv file: ' + csv_file)
                csv_data = csv.reader(open(csv_file, 'r'))

                try:
                    logging.info('Deleting old data from Database')
                    cur.execute('DELETE FROM ' + table_name)
                    cur.execute('ALTER TABLE ' + table_name + ' AUTO_INCREMENT = 1')
                except Exception:
                    logging.error('Attempt to delete old data in DB failed', exc_info=True)

                try:
                    logging.info('Writing newly scraped data to Database')
                    row_count = 0
                    for row in csv_data:
                        if row_count != 0:
                            # print(row)
                            cur.execute('INSERT INTO ' + table_name + ' (name, slug, image_url, image_alt, short_description, category, MRP, our_price, product_url, number_of_ratings, avg_rating, total_review, quantity) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', row)
                        row_count += 1
                except Exception:
                    logging.error('Inserting data to table failed.', exc_info=True)

                try:
                    logging.info('Committing data and closing connection.')
                    connection.commit()
                    cur.close()
                except Exception:
                    logging.error('Committing data and closing DB connection failed.')
        except Exception:
            logging.error('Connection with DB failed', exc_info=True)
