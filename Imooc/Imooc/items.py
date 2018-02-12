# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from w3lib.html import remove_tags

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join


def return_value(value):
    """
    将列表原值返回，避免默认out_processor取数组第一个
    """
    return value


def remove_space(value):
    """去除换行符和大量空白"""
    import re
    p = re.compile('\s+')
    value = [re.sub(p, '', i) for i in value if re.sub(p, '', i)]
    return value


class TakeIndex(object):
    """
    模拟数组切片，可传入1或2个参数
    """
    def __init__(self, start, end=None):
        self.start = start
        self.end = end

    def __call__(self, values):
        # 传入了2个值
        if self.end:
            return values[self.start:self.end]
        # 只传了一个值
        else:
            return values[self.start]


class ImoocItemloader(ItemLoader):
    default_output_processor = TakeFirst()


class TeacherItemloader(ItemLoader):
    default_output_processor = TakeFirst()


class CourseCommentItemloader(ItemLoader):
    default_output_processor = TakeFirst()


class CourseCommentItem(scrapy.Item):
    """
    每页有多条评论，因此需保留返回的数组而不使用TakeFirst()
    """
    comment_content = scrapy.Field(
        input_processor=MapCompose(remove_tags),
        output_processor=MapCompose(return_value)
    )
    comment_author = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    comment_time = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    comment_author_img = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    fav_nums = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    course = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = []
        params = []

        for content, author, time, fav_num, img in zip(
                      self.get('comment_content', []), self.get('comment_author', []), self.get('comment_time', []),
                      self.get('fav_nums', []), self.get('comment_author_img', [])
                    ):

            insert_sql.append("""
                          insert into comments(comment_content, comment_author, comment_time, 
                          fav_nums, comment_author_img, course)
                          VALUEs (%s, %s, %s,  %s, %s, %s)
                          ON DUPLICATE KEY UPDATE fav_nums=VALUES(fav_nums)
                          """)
            params.append((
                    content, author, time, fav_num, img, self.get('course', 0)
                        ))

        return insert_sql, params


class TeacherItem(scrapy.Item):
    id = scrapy.Field()
    teacher_name = scrapy.Field()
    teacher_img_url = scrapy.Field()
    teacher_position = scrapy.Field(
        input_processor=remove_space,
        output_processor=TakeIndex(-1)
    )
    teacher_desc = scrapy.Field()
    exp = scrapy.Field(
        output_processor=TakeIndex(0)
    )
    following = scrapy.Field(
        output_processor=TakeIndex(2)
    )
    fans = scrapy.Field(
        output_processor=TakeIndex(-1)
    )

    def get_insert_sql(self):
        insert_sql = []
        params = []

        insert_sql.append("""
                   insert into teacher(id, teacher_name, teacher_img_url, teacher_position, teacher_desc, exp, following, fans)
                   VALUEs (%s, %s, %s, %s, %s, %s, %s,%s)
                  ON DUPLICATE KEY UPDATE fans=VALUES(fans)
                   """)
        params.append((self.get('id', 0), self.get('teacher_name', 'N'), self.get('teacher_img_url', 'N'),
                       self.get('teacher_position', 'N'),  self.get('teacher_desc', 'N'),
                       self.get('exp', 0), self.get('following', 'N'), self.get('fans', 'N')
                       ))

        return insert_sql, params


class ImoocItem(scrapy.Item):
    id = scrapy.Field()
    url = scrapy.Field()
    course_name = scrapy.Field()
    course_img = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    course_img_path = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    students = scrapy.Field()
    degree = scrapy.Field(
        output_processor=TakeIndex(0)
    )
    learn_times = scrapy.Field(
        output_processor=TakeIndex(1)
    )
    # 综合评分
    score = scrapy.Field(
        output_processor=TakeIndex(-1)
    )
    course_desc = scrapy.Field()
    course_introduction = scrapy.Field()
    teacher_id = scrapy.Field()
    lessons = scrapy.Field(
        input_processor=remove_space,
        output_processor=MapCompose(return_value)
    )
    videos = scrapy.Field(
        input_processor=remove_space,
        output_processor=MapCompose(return_value)
    )
    video_urls = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    should_know = scrapy.Field(
        output_processor=TakeIndex(-1)
    )
    can_learn = scrapy.Field(
        output_processor=TakeIndex(-1)
    )
    course_label = scrapy.Field()
    category = scrapy.Field(
        output_processor=Join(',')
    )

    def get_insert_sql(self):
        insert_sql = []
        params = []

        insert_sql.append("""
                   insert into course(id, url, course_name, course_img, course_img_path, students, degree, learn_times,
                   score, course_desc, course_introduction, teacher_id, should_know, can_learn, course_label, category)
                   VALUEs (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)
                  ON DUPLICATE KEY UPDATE score=VALUES(score)
                   """)
        params.append((self.get('id', 0), self.get('url', ''), self.get('course_name', 'N'),  self.get('course_img', 'N'),  self.get('course_img_path', 'N'), self.get('students', 0), self.get('degree', 'N'),
                  self.get('learn_times', 'N'), self.get('score', 0), self.get('course_desc', 'N'), self.get('course_introduction', 'N'), self.get('teacher_id', 'N'), self.get('should_know', 'N'), self.get('can_learn', 'N'),  self.get('course_label', 'N'), self.get('category', 'N'))
                       )


        for lesson_name in self.get('lessons', []):
            insert_sql.append("""
                           insert into lesson(lesson_name, course_id)
                           VALUEs (%s, %s)
                          ON DUPLICATE KEY UPDATE lesson_name=VALUES(lesson_name)
                           """)
            params.append((lesson_name, self.get('id', 0)))


        for video_name, video_url in zip(self.get('videos', []), self.get('video_urls', [])):
            insert_sql.append("""
                              insert into video(video_name, course_id, video_url )
                              VALUEs (%s, %s, %s)
                              ON DUPLICATE KEY UPDATE video_name=VALUES(video_name)
                              """)
            params.append((video_name, self.get('id', 0), video_url) )

        return insert_sql, params
