"""Скрипт парсера данных по фильмам с сайта Кинопоиск"""

# Импортируем необходимые библиотеки

from bs4 import BeautifulSoup as bs     # Извлечение данных из файлов HTML и XML
import requests     # Для отправки GET запросов к интернет-ресурсам
import time     # Для формирования задержки между запросами
from pymongo import MongoClient     # Для взаимодействия с сервером MongoDB (база данных)


# Функция отправки GET запроса, возвращает результат запроса

def get_request(url, headers):
    r = requests.get(url, headers=headers)
    return r


# Функция "заливки" полученных данных в базу данных MongoDB

def add_films_to_db(db, films_list):
    for film in films_list:
        db.update_one({"$and": [{'film_name': {"$eq": film["film_name"]}}, {'url': {"$eq": film["url"]}}]},
                      {"$set": film}, upsert=True)


# Основная функция - извлечение данных из результата GET запроса
# Возвращает список словарей, где каждый словарь - набор данных по одному фильму

def film_year_data(year):

    # Главная ссылка на ресурс - "фундамент" каждого запроса

    main_link = 'https://www.kinopoisk.ru/'

    # На каждой странице, с которой будет происходить парсинг, размещены 100 фильмов
    # Т.к. отбираем 300 фильмов за каждый год, то и страницы три
    # Чтобы изменить количество, отбираемых фильмов достаточно, внести изменения в список "pages"

    pages = ['1', '2', '3']

    # На каждой итерации, формируем пустой список с фильмами

    films_list = []

    # С помощью цикла осуществляем "переключение" между страницами

    for page in pages:

        # Задержка между проведением запросов 5 сек., чтобы не навлечь гнев Кинопоиска :)

        time.sleep(5)

        # Формируем ссылку, к которой будет проводится запрос

        url = main_link + 's/type/film/list/1/m_act[year]/' + year + '/page/' + page

        # В параметре "headers" указываем User-Agent, чтобы сайт кинопоиска принял парсер за обычного
        # пользователя, использующего браузер

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/94.0.4606.61 Safari/537.36'
                  }

        # Отправляем запрос - получаем ответ

        r = get_request(url, headers)

        # Преобразуем ответ в необходимый для чтения формат

        soup = bs(r.text, 'lxml')

        # Ищем блок, в котором расположены все блоки фильмов страницы

        elements = soup.find_all(attrs={'class': 'element'})

        # С помощью цикла переключаемся между блоками фильмов

        for element in elements:

            # На каждом шаге формируется пустой словарь, чтобы записать в него инфо по новому фильму

            film_info = {}

            """Для сбора данных по фильму вводим конструкцию "try-except", 
                т.к. у некоторых фильмов могут отсутствовать те или иные данные"""

            # Название фильма
            try:
                film_info['film_name'] = element.find(attrs={'class': 'name'}).find('a').text
            except Exception as e:
                print(e)

            # Год выпуска
            film_info['film_year'] = year

            # Продолжительность
            try:
                film_info['film_time'] = element.find_all(attrs={'class': 'gray'})[0].text.split(', ')[-1]
            except Exception as e:
                print(e)

            # Страна
            try:
                film_info['country'] = element.find_all(attrs={'class': 'gray'})[1].text.split('реж.')[0]
            except Exception as e:
                print(e)

            # Главные роли
            try:
                film_info['main_roles'] = element.find_all(attrs={'class': 'gray'})[2].text
            except Exception as e:
                print(e)

            # Жанр фильма
            try:
                film_info['genre'] = element.find_all(attrs={'class': 'gray'})[1].text.split('(')[1]
            except Exception as e:
                print(e)

            # Режиссёр фильма
            try:
                film_info['director'] = element.find(attrs={'class': 'director'}).find('a').text
            except Exception as e:
                print(e)

            # Рейтинг фильма
            try:
                film_info['rating'] = element.find(attrs={'class': 'rating'}).text
            except Exception as e:
                print(e)

            # Ссылка на фильма
            try:
                film_href = element.find(attrs={'class': 'name'}).find('a').attrs['href']
                film_info['url'] = main_link + film_href
            except Exception as e:
                print(e)

            # Добавляем получившийся словарь в список фильмов

            films_list.append(film_info)

    # Возвращаем список фильмов собранных за указанный год (параметр "year")

    return films_list


# Конструкция, показывающая, что часть кода не должна выполнятся при импорте данного скрипта

if __name__ == "__main__":

    # Выполняем подключение к клиенту MongoDB (в данном случае локальный сервер)

    client = MongoClient('127.0.0.1', 27017)

    # Формируем список годов, по которым нам нужна информация

    years_list = [str(i) for i in range(2000, 2021)]

    # С помощью цикла переключаемся между годами

    for year in years_list:

        # Формируем для каждого года отдельную базу данных

        db_name = year + '_kinopoisk_top_300'
        db = client[db_name]
        films_db = db.films

        # Получаем список фильмов по каждому году из списка "years_list"

        films_list = film_year_data(year)

        # "Заливаем" полученные данные в MongoDB

        add_films_to_db(films_db, films_list)

        # Сообщение, для контроля

        print('фильмы ' + year + ' года добавлены')
