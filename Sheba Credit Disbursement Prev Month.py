#!/usr/bin/env python3
import mysql.connector
import pandas as pd
from datetime import datetime
from datetime import datetime, timedelta
import asyncio
from telegram import Bot
import warnings
import os
import tempfile
import sys

warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

# --- DB Credentials from environment variables ---
HOST = "sheba-xyz-prod-replica.clwkqg26yift.ap-southeast-1.rds.amazonaws.com"
USER = "pronoy_rdusr"
PORT = 3306
DATABASE = "sheba"
PASSWORD = os.environ.get("DB_PASSWORD")

# --- Telegram Bot credentials from environment variables ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")  # Make sure group ID is negative for groups

if not all([PASSWORD, BOT_TOKEN, CHAT_ID]):
    print("Missing one of the required environment variables: DB_PASSWORD, BOT_TOKEN, CHAT_ID")
    sys.exit(1)

try:
    CHAT_ID = int(CHAT_ID)
except ValueError:
    print("CHAT_ID must be an integer (negative for groups)")
    sys.exit(1)

# --- Today's date ---
today = datetime.today().strftime('%Y-%m-%d')

# --- SQL Query ---
query = """
SELECT 
    bonus_logs.*, 
    profiles.name,
    profiles.mobile
FROM bonus_logs
LEFT JOIN customers
    ON bonus_logs.user_id = customers.id
LEFT JOIN profiles
    ON customers.profile_id = profiles.id
WHERE 1=1
  WHERE 1=1
  AND DATE_FORMAT(DATE_SUB(bonus_logs.created_at, INTERVAL 6 HOUR), '%Y-%m') = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m')
"""

# --- Async Telegram send function ---
async def send_to_telegram(file_path: str):
    try:
        bot = Bot(token=BOT_TOKEN)
        with open(file_path, "rb") as f:
            await bot.send_document(chat_id=CHAT_ID, document=f, filename=os.path.basename(file_path))
        print("Report sent to Telegram group")
    except Exception as e:
        print("Failed to send report to Telegram:", e)

# --- Main execution ---
def main():
    conn = None
    try:
        conn = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE,
            port=PORT
        )
        print("Connected to MySQL")

        df = pd.read_sql(query, conn)
        print(f"Fetched {len(df)} rows")

        # --- Clean illegal Excel characters ---
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.replace(r'[\x00-\x1F]', '', regex=True)

        # --- Save Excel in temporary directory ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # safe_filename = f"FIN_Data_PREVMONTH_{timestamp}.xlsx"

        # Get previous month and year
        prev_month_date = datetime.now().replace(day=1) - timedelta(days=1)
        month_name = prev_month_date.strftime("%B")   # Full month name, e.g., "October"
        year = prev_month_date.strftime("%Y")

        # Create filename
        safe_filename = f"Sheba_Credit_Disbursement_{month_name}_{year}.xlsx"


        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, safe_filename)
        df.to_excel(file_path, index=False)
        print(f"Report saved as {file_path}")

        # --- Send to Telegram ---
        asyncio.run(send_to_telegram(file_path))

    except mysql.connector.Error as e:
        print("MySQL error:", e)

    except Exception as e:
        print("Unexpected error:", e)

    finally:
        if conn and conn.is_connected():
            conn.close()
            print("Connection closed")
        print("---------------------------------------------------------------")

if __name__ == "__main__":
    main()
