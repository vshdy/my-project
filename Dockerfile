# Используем легкий образ Python
FROM python:3.10-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# список зависимостей
COPY requirements.txt .

# библиотеки
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект в контейнер
COPY . .

# Открываем порт для Streamlit
EXPOSE 8501

CMD ["streamlit", "run", "dashboards/app.py", "--server.port=8501", "--server.address=0.0.0.0"]