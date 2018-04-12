import logging
import conn
import function as fc


def update():
    sql=""
    try:
        conn.mysql(sql)
        return True
    except:
        return False


def parser_two_detail:


def parser_two_main:


def parser_one_detail:


def parser_one_main:


def do_main:
    community_list = conn.get_all("select * from inf_community where status=1")
    for community in community_list:
        city_code = community[1]
        community_code = community[2]

        page_path = f'data/city/{city_code}/community/{community_code}.html'
        page = fc.read_page(page_path)
        if page is False:
            continue

        if page == '':
            parser_one_main(page)
        if page == '':
            parser_two_main(page)
        logging.warning(f"未识别页面,无法选择解析器.")
        continue

if __name__ == '__main__':
    do_main()
