import logging
import conn
import function as fc
import math
from multiprocessing import Process


def do_fetch_housing(url, city_code, city_name, process_num):
    # 获取小区页面做检测
    if not fc.fetch_page(url, f'data/temp/{process_num}/temp.html'):
        return False
    page = fc.read_page(f"data/temp/{process_num}/temp.html", "GBK")

    page_title = page('title').text()
    if page_title.find(city_name) == -1:
        logging.warning(f"city_name:{city_name},city_code:{city_code},webtitle != city_name")
        conn.mysql(f"update inf_city set status=8 where city_code='{city_code}'")
        return False

    # 获取小区页面最大分页值，如果没有分页则说明没有收录小区信息
    page_split = page('#houselist_B14_01')
    if page_split.html() is None or page_split.html() == 'None':
        logging.warning(f'没有找到小区信息: {url}')
        conn.mysql(f"update inf_city set status = 9 where city_code = '{city_code}' and website='房天下'")
        return False
    page_split = page_split('.txt').text()
    page_split_max = int(page_split.strip('共页'))
    # 抓取小区各个分页
    for num in range(1, page_split_max + 1):
        split_url = url + '__0_0_0_0_' + str(num) + '_0_0_0/'
        file_path = f'data/city/{city_code}/index/{city_code}_{str(num)}.html'
        if not fc.fetch_page(split_url, file_path):
            return False
    conn.mysql(f"update inf_city set status = 1 where city_code = '{city_code}' and website='房天下'")
    return True


def vprocess(process_num, city):
    conn.link()
    for City_code in city:
        city_code = City_code[0]
        city_name = City_code[1]
        if city_code == 'bj':
            url = 'http://esf.fang.com/housing/'
            # 北京的小区信息不在bj域名下
        else:
            url = 'http://esf.' + city_code + '.fang.com/housing/'
        do_fetch_housing(url, city_code, city_name, process_num)
    conn.close()
    logging.info(f"进程 {process_num} 结束.")


def do_fetch():
    conn.link()
    all_city_code = conn.get_all("select city_code,city_name from inf_city where status = 0 and website='房天下'")
    conn.close()
    if all_city_code == ():
        return False

    count_city = len(all_city_code)
    logging.info(f"总计有 {count_city} 个城市.")

    process_part = fc.process_part
    process_num = math.ceil(count_city / process_part)
    if process_num > fc.process_num:
        process_part = math.ceil(count_city / fc.process_num)
        process_num = fc.process_num

    part_community = [[0 for col in range(process_part)] for row in range(process_num)]
    for i, a_community in enumerate(all_city_code):
        fpart = int(i / process_part)
        spart = i - fpart * process_part
        part_community[fpart][spart] = a_community

    for _process_num in range(fpart + 1):
        logging.info(f"启动进程 {_process_num}")
        p = Process(target=vprocess, args=(process_num, part_community[_process_num],))
        p.start()

    return True


if __name__ == '__main__':
    # conn.link()
    # for x in range(1, 4):
    #     if not do_fetch():
    #         break
    do_fetch()
    # logging.info(f"community_list 抓取完毕,循环抓取了{x}次.")
    # conn.close()
