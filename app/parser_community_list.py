import logging
import conn
import os
import time
from pathlib import Path
import function as fc
from pyquery import PyQuery as pq
import math
from multiprocessing import Process


def vprocess(process_num, city):
    conn.link()
    for City_code in city:
        if City_code == 0:
            continue
        city_code = City_code[0]
        # 获取城市索引下面所有分页
        files = fc.get_all_files(f'data/city/{city_code}/index')

        for file in files:
            file_path = f'data/city/{city_code}/index/{file}'
            file_full_path = Path(__file__).parent.parent.joinpath(f'data/city/{city_code}/index/{file}')
            if not os.path.isdir(file_full_path):
                logging.info(f"读取文件:{file}")
                page_main = fc.read_page(file_path, "GBK")
                # 截取主体部分
                page_main = page_main('.houseList').html()
                if page_main is None or page_main == "":
                    logging.warning(f"{file}页面没有主体部分")
                    continue
                # 抓取每条信息更新到数据库

                # 可能会出现异常页面如sh_76 主体部分内容为空的,房天下的页面经常更新的，内容会变
                try:
                    page_main = pq(page_main)
                except Exception as e:
                    logging.warning(f"读取主体部分失败, message={e}")
                    continue
                page_main = page_main.find("div[class='list rel']")

                for community in page_main.items():
                    community_img_url = community('dl>dt>a>img').attr('src')
                    community_url = community('dl>dd>p>a').attr('href')
                    community_code = fc.get_city_code(community('dl>dd>p>a').eq(0).attr('href'))
                    if community_code is False:
                        continue
                    community_name = community('dl>dd>p>a').eq(0).text()
                    community_type = community('dl>dd>p>span').eq(0).text()
                    community_level = community('dl>dd>p>span').eq(1).html()
                    if community_level is not None:
                        community_level = community_level.count('<i/>') + 0.5 * community_level.count('<i'
                                                                                                      ' class="half"/>')
                    else:
                        community_level = 'Null'
                    community_location = fc.remove_html(community('dl>dd>p').eq(1).html(), 1)
                    community_location = community_location.lstrip(' ')
                    community_location = community_location.rstrip(' ')
                    community_selling = fc.remove_html(community('dl>dd>ul>li').eq(0).html())
                    community_selling = community_selling.replace('套在售', '')
                    community_selling = community_selling.replace('|', '')
                    community_leasing = fc.remove_html(community('dl>dd>ul>li').eq(1).html())
                    community_leasing = community_leasing.replace('套在租', '')
                    community_leasing = community_leasing.replace('|', '')
                    community_build_time = community('dl>dd>ul>li').eq(2).html()
                    community_price = fc.remove_html(community.find("p[class='priceAverage']").html())
                    outfor = 0
                    for x in range(100):
                        temp = conn.get_one(f"select * from inf_community where community_code = '{community_code}'"
                                            f" and website = '房天下' and status<>-1 order by fetch_time desc limit 1")
                        # 处理重复
                        if temp is not None:
                            fetch_time = temp[6]
                            fetch_time = fetch_time.strftime('%Y-%m')
                            this_moon = time.strftime('%Y-%m')
                            if temp[5] == community_url:
                                # code 相同 url 相同的情况下，说明是同一个数据，只判断时间是否当月，当月的执行update,
                                # 并继续大循环，非当月则跳出这个循环，执行 insert
                                # todo 加入完全匹配，如果数据完全一致则不进行 update 减少日志warning输出,如果 city_code 变了抛出提醒,并且不更新数据
                                if this_moon == fetch_time:
                                    record = f"{time.strftime('%Y-%m-%d %H:%M:%S')}:ID:{temp[0]}[{temp[3]}" \
                                             f"->{community_name}]\n{temp[9]}"
                                    sql = f"update inf_community set city_code='{city_code}'," \
                                          f"community_name='{community_name}'," \
                                          f"url='{community_url}',update_time='{time.strftime('%Y-%m-%d %H:%M:%S')}'," \
                                          f"status=0,status_record='{record}',img_url='{community_img_url}'," \
                                          f"type='{community_type}',level={community_level}," \
                                          f"location='{community_location}',selling={community_selling}," \
                                          f"leasing={community_leasing},build_time='{community_build_time}'," \
                                          f"price='{community_price}',status=0" \
                                          f" where community_code='{community_code}'" \
                                          f" and website='房天下' and status<>-1"
                                    conn.mysql(sql)
                                    logging.warning(f"ID:{temp[0]}:community_name [{temp[3]}->{community_name}]")
                                    outfor = 1
                                break
                            # code 相同 url 不同, code + T 继续循环查询新code
                            community_code = str(community_code) + 'T'
                            continue
                        # 无重复 结束循环直接 insert
                        break

                    if outfor == 1:
                        continue

                    sql = f"insert into inf_community" \
                          f" (city_code,community_code,community_name,website,url,fetch_time,img_url,type,level" \
                          f",location,selling,leasing,build_time,price)" \
                          f" values" \
                          f" ('{city_code}','{community_code}','{community_name}','房天下','{community_url}'" \
                          f",'{time.strftime('%Y-%m-%d %H:%M:%S')}','{community_img_url}','{community_type}'," \
                          f"{community_level},'{community_location}',{community_selling}," \
                          f"{community_leasing},'{community_build_time}','{community_price}')"
                    conn.mysql(sql)
        conn.mysql(f"update inf_city set status = 2 where city_code = '{city_code}' and website='房天下'")
    conn.close()
    logging.info(f"进程 {process_num} 结束.")


def do_parser():
    conn.link()
    all_city_code = conn.get_all("select city_code from inf_city where status = 1 and website='房天下'")
    conn.close()
    if all_city_code == ():
        return False

    count_city = len(all_city_code)

    process_part = math.ceil(count_city / fc.process_max_num)
    process_num = fc.process_max_num
    if count_city < process_num:
        process_num = count_city

    part_community = [[0 for col in range(process_part)] for row in range(process_num)]
    for i, a_community in enumerate(all_city_code):
        fpart = int(i / process_num)
        spart = i - fpart * process_num
        part_community[spart][fpart] = a_community

    for _process_num in range(process_num):
        logging.info(f"启动进程 {_process_num}")
        p = Process(target=vprocess, args=(_process_num, part_community[_process_num],))
        p.start()


if __name__ == '__main__':
    do_parser()

