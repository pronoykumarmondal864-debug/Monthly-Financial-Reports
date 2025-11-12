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
/* Rezwan-ul-Akbar */
SELECT 
        order_code, partner_order_payments.id payment_id,  sp_id, sp_name, 
        CASE
    WHEN master_category_id IN (703) THEN 'digiGO'
    WHEN order_media IN ('B2B') THEN 'sBusiness'
    WHEN order_media = 'E-Shop' AND master_category_id = 802 THEN 'SP Shop'
    WHEN master_category_id IN (184,868) THEN 'Laundry'
    WHEN master_category_id IN (185,266,829,845,847) THEN 'sCatering'
    WHEN master_category_id IN (7, 9, 94, 101, 104, 123, 133, 286, 287, 288, 289, 290, 291, 296, 335, 336, 337, 344, 348, 353,387, 391, 411, 441, 433, 455, 549, 581, 627, 636, 686, 802, 859, 873) THEN 'New Business'
    WHEN master_category_id IN (317, 745) THEN 'Legal Service'
    WHEN master_category_id IN (383) THEN 'sDelivery'
    WHEN service_category_id IN (835,834) THEN 'Akash DTH'
    WHEN master_category_id IN (338) THEN 'sMarket'
    WHEN order_media LIKE '%bondhu%' THEN 'Bondhu Referral'
    WHEN master_category_id IN (333, 387, 757, 770, 772, 774, 776, 792, 811, 833, 841) THEN 'Product Reselling'
    WHEN location IN (SELECT name FROM locations WHERE ID IN (120,121,122,123,126,127,128,129,130,131,132,133,134,135,136,137,142,143,144,145,146,147,148,149,150,151,152,153)) THEN 'Chittagong'
    WHEN master_category_id = 859 THEN 'New Projects'
    WHEN service_category_id = 655 THEN 'Housemaid'
    WHEN master_category_id IN (562,873) THEN  'Sheba Shop'
    WHEN master_category_id IN (1,2,3,4,5,6,8,73,84,91,183,186,221,224,225,226,235,236,237,240,334,365,416,505,537,544,596,599,619,621,624,635,695,714,786,790,818) THEN 'MX Exact'
ELSE 'Unidentified'
END Divisions,
        order_first_created, closed_date order_close_date, partner_order_payments.created_at payment_date, partner_order_payments.created_by_name collected_by, payment_status, transaction_type, method, amount
FROM partner_order_payments LEFT JOIN partner_order_report ON partner_order_payments.partner_order_id = partner_order_report.id
WHERE 1=1 
AND DATE_FORMAT(created_at, '%Y-%m') = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m')
GROUP BY order_code, partner_order_payments.id 
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
        safe_filename = f"Collection_method_with_collected_by_{month_name}_{year}.xlsx"


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
