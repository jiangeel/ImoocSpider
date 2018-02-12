# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.images import ImagesPipeline
from twisted.enterprise import adbapi
import pymysql

class ImoocPipeline(object):
    def process_item(self, item, spider):
        return item


class MysqlTwistedPipeline(object):
    # 采用异步写入 mysql
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbparms = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True
        )

        dbpool = adbapi.ConnectionPool('pymysql', **dbparms)

        return cls(dbpool)

    def process_item(self, item, spider):
        # 使用 twisted 将 mysql 插入变成异步执行
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item, spider)  # 处理异常

    def handle_error(self, failure, item, spider):
        # 处理异步插入的异常
        print(failure)

    def do_insert(self, cursor, item):
        # 根据不同 item 构建不同的 sql 语句并插入 mysql 中
        insert_sql, params = item.get_insert_sql()
        # 将返回多对 sql和params，分别插入数据库
        list(map(cursor.execute, insert_sql, params))
        # cursor.execute(insert_sql, params)


# 抓取图片的 pipline
class CourseImagePipeline(ImagesPipeline):
    # 重载item_completed函数
    def item_completed(self, results, item, info):
        if 'course_img' in item:
            for ok, value in results:
                image_file_path = value["path"]
            item['course_img_path'] = image_file_path

        return item
