### О проекте: 

**Сервис для отслеживания изменений в базе данных PostgreSQL и отправки этих изменений в Elasticsearch. Для хранения текущего состояния загрузки реализовано хранилище информации, использующее локальный файл. Формат хранения: JSON**

**Используемые технологии:**

- pydantic
- elasticsearch
- psycopg

### Как развернуть проект:

Склонируйте проект

```
git clone https://github.com/VladimirNagibin/new_admin_panel_sprint_3
```

Перейдите в рабочую папку
```
cd new_admin_panel_sprint_3/
```

Создайте файл .env 
```
touch .env
```

Заполните файл .env по шаблону .env.example

Запустите приложение
```
sudo docker compose up
```

После запуска и загрузки тестовых данных в PostgreSQL из файла /postgres/database_dump.sql начнётся процесс проверки изменений в базе PostgreSQL и загрузка в Elasticsearch.

Для тестирования работы Elasticsearch открыт порт 9200. 
Пример запроса к Elasticsearch: http://localhost:9200/movies/_search
____

**Владимир Нагибин** 

Github: [@VladimirNagibin](https://github.com/VladimirNagibin/)