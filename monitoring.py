import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google_play_scraper import app
from concurrent.futures import ThreadPoolExecutor
import schedule
import time
from datetime import datetime

# 🔄 Подключение к Google Sheets через GitHub Secrets
print("🔄 Подключаемся к Google Sheets...")
creds_json = json.loads(os.getenv("GOOGLE_CREDENTIALS"))  # Загружаем ключи из секретов GitHub
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
client = gspread.authorize(creds)

spreadsheet_id = "1DpbYJ5f6zdhIl1zDtn6Z3aCHZRDFTaqhsCrkzNM9Iqo"  # ID таблицы
sheet = client.open_by_key(spreadsheet_id).sheet1  # Основная таблица

# Кешируем данные таблицы, чтобы не делать лишние запросы
all_values = sheet.get_all_values()
apps_google_play = all_values[1:]  # Убираем заголовок

# Проверяем, существует ли лист "Changes Log", если нет – создаём
try:
    log_sheet = client.open_by_key(spreadsheet_id).worksheet("Changes Log")
except gspread.exceptions.WorksheetNotFound:
    print("❌ Лист 'Changes Log' не найден, создаём его...")
    log_sheet = client.open_by_key(spreadsheet_id).add_worksheet(title="Changes Log", rows="1000", cols="4")
    log_sheet.append_row(["Дата изменения", "Тип изменения", "Номер приложения", "Package"])  # Заголовки

# Функция записи изменений в лог (batch update)
log_buffer = []

def log_change(change_type, app_number, package_name):
    print(f"📌 Логируем: {change_type} - {package_name}")  
    log_buffer.append([datetime.today().strftime("%Y-%m-%d"), change_type, app_number, package_name])

# Функция отправки логов в Google Sheets (уменьшение нагрузки на API)
def flush_log():
    global log_buffer
    if log_buffer:
        try:
            log_sheet.append_rows(log_buffer)
            print(f"✅ В лог записано {len(log_buffer)} изменений.")
            log_buffer = []  # Очищаем буфер после записи
        except Exception as e:
            print(f"❌ Ошибка записи в 'Changes Log': {e}")

# Функция проверки приложений
def fetch_google_play_data(package_name, app_number, existing_status, existing_release_date, existing_not_found_date):
    try:
        print(f"🔍 Проверяем {package_name}...")

        time.sleep(0.5)
        data = app(package_name)

        status = "ready"

        release_date = data.get("released")
        last_updated = data.get("updated")

        def convert_timestamp(value):
            if isinstance(value, int) and value > 1000000000:
                return datetime.utcfromtimestamp(value).strftime("%Y-%m-%d")
            return value

        release_date = convert_timestamp(release_date)
        last_updated = convert_timestamp(last_updated)

        final_date = release_date if release_date else last_updated or "Не найдено"
        not_found_date = ""

        print(f"📅 Дата {package_name}: {final_date}")
        print(f"🔄 {existing_status} → {status}")

        # Логика записи изменений
        if existing_status in ["", None]:  
            log_change("Загружено новое приложение", app_number, package_name)
        elif existing_status == "ban" and status == "ready":
            log_change("Приложение появилось в сторе", app_number, package_name)
        elif existing_status == "ready" and status == "ban":
            log_change("Бан приложения", app_number, package_name)

        return [package_name, status, final_date, not_found_date]

    except Exception as e:
        print(f"❌ Ошибка при проверке {package_name}: {e}")
        status = "ban"
        not_found_date = existing_not_found_date or datetime.today().strftime("%Y-%m-%d")

        # Логируем бан только если раньше приложение было доступно
        if existing_status not in ["ban", None, ""]:
            log_change("Бан приложения", app_number, package_name)

        return [package_name, status, existing_release_date, not_found_date]

# **Функция проверки всех приложений**
def fetch_all_data():
    print("🚀 Запуск проверки всех приложений...")
    apps_list = []

    for row in apps_google_play:
        if len(row) >= 8 and row[7]:  # Убедимся, что есть пакет
            apps_list.append((row[0], row[7], row[3], row[5], row[6]))

    print(f"✅ Найдено {len(apps_list)} приложений для проверки.")
    with ThreadPoolExecutor(max_workers=5) as executor:
        return list(executor.map(lambda x: fetch_google_play_data(x[1], x[0], x[2], x[3], x[4]), apps_list))

# **Обновление данных в Google Sheets**
def update_google_sheets(sheet, data):
    print("🔄 Обновляем данные в Google Sheets...")

    updates = []
    ready_count = 0  

    for i, row in enumerate(apps_google_play, start=2):  # Начинаем с 2-й строки
        package_name = row[7]
        for app_data in data:
            if app_data[0] == package_name:
                updates.append({"range": f"D{i}", "values": [[app_data[1]]]})
                updates.append({"range": f"F{i}", "values": [[app_data[2]]]})
                updates.append({"range": f"G{i}", "values": [[app_data[3]]]})

                if app_data[1] == "ready":
                    ready_count += 1
                break

    if updates:
        try:
            sheet.batch_update(updates)
            print(f"✅ Данные обновлены. Доступных приложений: {ready_count}")
        except Exception as e:
            print(f"❌ Ошибка обновления данных: {e}")

    # Обновляем количество доступных приложений
    try:
        sheet.update(range_name="J2", values=[[ready_count]])
    except Exception as e:
        print(f"❌ Ошибка обновления счетчика доступных приложений
