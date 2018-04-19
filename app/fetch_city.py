import logging
import conn
import re
import time
import function as fc
from pyquery import PyQuery as pq


def do_fetch():
    fc.fetch_page("http://www.fang.com/SoufunFamily.htm", "data/temp/index.html")

    page = fc.read_page("data/temp/index.html", "GBK")
    page = str(page('#c02').html())
    page = re.sub(r'<!--[^>]((?:.|\n)*?)-->', '', page)  # 删除多行注释
    # page = re.sub(r'<!--[^>]*-->', '', page)  替换单行注释
    page = pq(page)

    city_list = page('a')
    for city_info in city_list.items():
        city_name = city_info.text()
        href = str(city_info.attr('href'))
        city_code = fc.get_city_code(href)

        if city_code is False:
            continue

        if not fc.fetch_page(href, "data/temp/temp.html"):
            continue

        cl_page = fc.read_page("data/temp/temp.html", "GBK")
        page_title = cl_page('title').text()
        if page_title.find(city_name) == -1:
            logging.warning(f"city_name:{city_name},city_code:{city_code},webtitle 和 city_name 不匹配")
            continue

        temp = conn.get_one(f"select id,city_name,url,status_record "
                            f"from inf_city where city_code = '{city_code}' and website = '房天下'"
                            f" and status<>-1")
        if temp is not None:
            t_name = temp[1]
            t_url = temp[2]
            record = temp[3]

            if city_name != t_name:
                record = f"{time.strftime('%Y-%m-%d %H:%M:%S')}:city_name [{t_name}->{city_name}]\n{record}"
                conn.mysql(f"update inf_city set city_name='{city_name}'"
                           f",update_time='{time.strftime('%Y-%m-%d %H:%M:%S')}'"
                           f",status_record='{record}' where id = '{temp[0]}'")
                logging.warning(f"ID:{temp[0]}:city_name [{t_name}->{city_name}]")

            if href != t_url:
                record = f"{time.strftime('%Y-%m-%d %H:%M:%S')}:url [{t_url}->{href}]\n{record}"
                conn.mysql(f"update inf_city set url='{href}'"
                           f",update_time='{time.strftime('%Y-%m-%d %H:%M:%S')}'"
                           f",status_record='{record}' where id = '{temp[0]}'")
                logging.warning(f"ID:{temp[0]}:url [{t_url}->{href}]")

            continue

        sql = f"insert into inf_city (city_code, city_name, website, webtitle, url, insert_time)" \
              f" values ('{city_code}','{city_name}', '房天下', '{page_title}'," \
              f" '{href}', '{time.strftime('%Y-%m-%d %H:%M:%S')}') "
        conn.mysql(sql)


if __name__ == '__main__':
    conn.link()

    sql = "update inf_city set status=0 WHERE status<>-1 and website='房天下'"
    conn.mysql(sql)
    logging.info("重置所有城市状态 status = 0")

    do_fetch()
    logging.info("inf_city 更新完毕.")
    conn.close()
