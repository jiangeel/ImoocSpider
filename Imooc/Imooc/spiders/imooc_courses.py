# -*- coding: utf-8 -*-
import scrapy
from items import ImoocItemloader, CourseCommentItemloader, TeacherItemloader
from items import ImoocItem, CourseCommentItem, TeacherItem
from urllib import parse


class ImoocCoursesSpider(scrapy.Spider):
    name = 'imooc_courses'
    allowed_domains = ['www.imooc.com']
    index = 1;
    url = 'https://www.imooc.com/course/list?page='
    start_urls = [url+str(index)]

    headers = {
        # "HOST": "www.imooc.com/",
        'User-Agent': "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0"
    }

    def start_requests(self):
        return [scrapy.Request(url=self.start_urls[0], dont_filter=True, headers=self.headers)]

    def parse(self, response):
        # 慕课网免费课程共33页
        self.index += 1
        print('visit:'+self.url+str(self.index))
        yield scrapy.Request(url=self.url+str(self.index), dont_filter=True, headers=self.headers)

        # 每页有5x5=25门课程
        courses = response.css('.course-card-container')
        for course in courses:
            course_url = course.css('a::attr(href)').extract_first()
            course_url = parse.urljoin(response.url, course_url)
            course_img = course.css('img::attr(src)').extract_first()
            course_img = parse.urljoin(response.url, course_img)
            course_label = course.css('label::text').extract()
            course_label = ','.join(course_label)
            students = course.css('.course-card-info span::text').extract()[-1]
            course_desc = course.css('.course-card-desc::text').extract_first()
            # 解析课程主要内容
            yield scrapy.Request(url=course_url, dont_filter=True, headers=self.headers, callback=self.parse_course,
                                 meta={'course_img': course_img, 'course_label': course_label,
                                       'course_desc': course_desc, 'students': students})

            # 解析评论页,每个课程只取5页
            for i in range(5):
                course_comment_url = course_url.replace('learn', 'course/comment/id')+'?page={0}'.format(i)
            yield scrapy.Request(url=course_comment_url, dont_filter=True, headers=self.headers, callback=self.parse_comment)

    def parse_course(self, response):
        """解析课程主要界面"""
        item_loader = ImoocItemloader(item=ImoocItem(), response=response)
        item_loader.add_css("course_name", ".hd h2::text")
        item_loader.add_value("url", response.url)
        id = response.url.split('/')[-1]
        item_loader.add_value("id", id)
        item_loader.add_value('course_img', [response.meta.get('course_img', 'N')])
        item_loader.add_value("students", int(response.meta.get('students', 10086)))
        item_loader.add_css("degree", ".static-item .meta-value::text")
        item_loader.add_css("learn_times", ".static-item .meta-value::text")
        item_loader.add_css("degree", ".static-item .meta-value::text")
        item_loader.add_css("score", ".static-item .meta-value::text")
        item_loader.add_value("course_desc", response.meta.get('course_desc', 'N'))
        item_loader.add_css("course_introduction", ".auto-wrap::text")
        item_loader.add_value("course_label", response.meta.get('course_label', 'N'))
        teacher_url = response.css('.tit a::attr(href)').extract_first()
        teacher_id = teacher_url.split('/')[2]
        item_loader.add_value("teacher_id", teacher_id)
        item_loader.add_css("should_know", ".first dd::text")
        item_loader.add_css("can_learn", ".course-info-tip dd::text")
        item_loader.add_css("category", ".path a::text")
        item_loader.add_css("lessons", ".chapter h3 strong::text")
        item_loader.add_css("videos", ".video a::text")
        item_loader.add_css("video_urls", '.video li a::attr(href)')

        imooc_item = item_loader.load_item()
        yield imooc_item

        # 爬取教师信息
        yield scrapy.Request(url=parse.urljoin(response.url, teacher_url), dont_filter=True, headers=self.headers, callback=self.parse_teacher)

    def parse_teacher(self, response):
        """解析教师信息"""
        item_loader = TeacherItemloader(item=TeacherItem(), response=response)

        # 精英教师的特殊界面
        if 'imooc.com/t/' in response.url:
            item_loader.add_css('teacher_name', '.tea-nickname::text')
            item_loader.add_css('id', '.js-add-attention::attr(data-id)')
            teacher_img_url = response.css('.tea-header::attr(src)').extract_first()
            item_loader.add_value('teacher_img_url', parse.urljoin(response.url, teacher_img_url))
            item_loader.add_css('teacher_position', '.tea-professional::text')
            item_loader.add_css('teacher_desc', '.tea-desc::text')
            item_loader.add_value('exp', 666)
            # 精英教师没有关注数量，随便写一个，但item中需要索引下标2故传入数组
            item_loader.add_value('following', [666,666,666])
            item_loader.add_css('fans', '#js-tea-fan-num::text')
        # 普通教师界面
        else:
            item_loader.add_css('id', '.js-add-follow::attr(data-uid)')
            item_loader.add_css('teacher_name', '.user-name span::text')
            teacher_img_url = response.css('.user-pic-bg img::attr(src)').extract_first()
            item_loader.add_value('teacher_img_url', parse.urljoin(response.url, teacher_img_url))
            item_loader.add_css('teacher_position', '.about-info span::text')
            item_loader.add_css('teacher_desc', '.user-desc::attr(title)')
            item_loader.add_css('exp', '.study-info em::text')
            item_loader.add_css('following', '.study-info em::text')
            item_loader.add_css('fans', '.study-info em::text')

        teacher_item = item_loader.load_item()
        yield teacher_item

    def parse_comment(self, response):
        """解析课程评论"""

        item_loader = CourseCommentItemloader(item=CourseCommentItem(), response=response)
        course_note = response.css('#course_note p::text').extract_first()
        if '此课程暂无同学评论' not in course_note:
            item_loader.add_css('comment_author', '.post-row .tit a::text')
            item_loader.add_css('comment_content', '.post-row .bd p')
            item_loader.add_css('comment_time', '.post-row .bd .footer .timeago::text')
            author_imgs = response.css('.post-row .media img::attr(src)').extract()
            author_img = [parse.urljoin(response.url, author_img) for author_img in author_imgs]
            item_loader.add_value('comment_author_img', author_img)
            item_loader.add_css('fav_nums', '.post-row .bd .footer em::text')
            course_id = response.css('#learnOn::attr(href)').extract_first()
            course_id = course_id.split('/')[-1]
            item_loader.add_value('course', course_id)

            course_comment_item = item_loader.load_item()
            yield course_comment_item
