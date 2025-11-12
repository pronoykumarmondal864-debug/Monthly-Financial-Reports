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
    order_unique_id, order_code, SBU, order_media, master_category_id, master_category_name, service_category_name, services,
    order_first_created, schedule_date, schedule_time, cancelled_date, closed_date as served_date,  DATE_FORMAT(closed_date, '%Y-%m') AS served_month_year, 
    customer_name, resource_id, resource_name, sp_name, sp_mobile, location, promo,  gmv,
    discount, discount_sheba, discount_partner, billed_amount, revenue AS GR, ROUND(SUM(Served_NR)) as NR,  
    status, job_status, collection, due, collected_sp, collected_sheba, service_charge_percentage,
    gmv_service, gmv_material, sp_cost, sp_cost_service, 
    
    SUM(
    IF(service_id LIKE '%2392%' AND sp_cost != 0, sp_cost_service + sp_cost_additional + sp_cost_delivery + discount_partner, 
    IF(service_id LIKE '%3446%' AND sp_cost != 0, sp_cost_service + sp_cost_additional + sp_cost_delivery + discount_partner, 
	IF(service_id LIKE '%2392%', gmv/8700*6200, 
    IF(service_id LIKE '%3446%', gmv/10250*6800, 
    IF(service_category_id != 562, sp_cost_service + sp_cost_additional + sp_cost_delivery + discount_partner, 
    IF(order_code = 'D-443541-154790', 0, 
    IF(closed_date BETWEEN '2020/15/01' AND '2021/02/01', (gmv/36900) * 31900, sp_cost_service + sp_cost_additional + sp_cost_delivery + discount_partner)))))))) as SP_Share,
    COD, bKash, Online, Sheba_Credit, ShebaPay
FROM 
   (SELECT 
        order_unique_id, order_code, master_category_id, master_category_name, service_category_name, service_category_id, services,
        created_date, schedule_date, schedule_time, cancelled_date, closed_date,  DATE_FORMAT(closed_date, '%Y-%m') AS served_Month_Year, 
        customer_name, resource_id, resource_name, order_first_created, sp_name, sp_mobile, location, promo, service_id, 
        discount, discount_partner, discount_sheba, billed_amount, gmv, revenue,  ROUND(revenue/105*100) AS Served_NR,
        status, job_status, collection, due, collected_sp, collected_sheba, service_charge_percentage,  
        sp_cost, sp_cost_service, sp_cost_additional, sp_cost_delivery, gmv_service, gmv_material,
        CASE
              WHEN (order_portal = 'resource-app' AND order_media = 'web') THEN 'sPro'
              WHEN (order_portal = 'web' AND order_media != 'resource-app') THEN 'Web'
              ELSE order_media
        END AS order_media, 
        CASE
    WHEN master_category_id IN (714,818) THEN  'AC Care'
    WHEN master_category_id IN (1, 2, 73, 84, 91, 225, 240, 624) THEN  'Appliance Solution'
    WHEN master_category_id IN (3) THEN  'Shifting Solution'
    WHEN master_category_id IN (4, 5, 6, 221, 224, 236, 365, 505, 599, 621, 635, 235, 596,786,790) THEN  'Vehicle Solution'
    WHEN master_category_id IN (183, 334, 544, 695) THEN  'Glow by Sheba'
    WHEN master_category_id IN (8, 186, 237, 940) THEN  'Cleaning Solution' 
    WHEN master_category_id IN (226, 416, 619, 537) THEN  'Renovation Solution'
    WHEN master_category_id IN (185,266) THEN  'sCatering'
    WHEN master_category_id IN (184,662,868) THEN  'Laundry'
    WHEN master_category_id IN (887) THEN  'Sheba Laundry'
    WHEN master_category_id IN (703) THEN  'DigiGo'
    WHEN master_category_id IN (802) THEN  'SP Shop'
    WHEN master_category_id IN (562,873) THEN  'Sheba Shop'
    WHEN master_category_id IN (918) THEN  'Best Deal'
    WHEN master_category_id IN (962) THEN  'Health and Care'
    WHEN master_category_id IN (978) THEN  'Maid Service'
    WHEN master_category_id IN (1009, 1010) THEN 'Brand Revenue'
ELSE 'Others'
END SBU, 
        SUM(IF(method IN('Cash On Delivery'),amount,0)) COD, 
        SUM(IF(method IN('Bkash'),amount,0)) bKash, 
        SUM(IF(method IN('Cbl', 'Cheque', 'Partner_wallet', 'Ssl', 'Transfer'),amount,0)) Online, 
        SUM(IF(method IN('Bonus', 'Wallet'),amount,0)) Sheba_Credit, 
        SUM(IF(method IN('ShebaPay'),amount,0)) ShebaPay -- Move this inside the subquery
FROM partner_order_report

LEFT JOIN partner_order_payments ON partner_order_payments.partner_order_id = partner_order_report.id

WHERE 1 = 1 
    AND DATE_FORMAT(closed_date, '%Y-%m') = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m')
    AND master_category_id IN (978, 962, 887, 873, 818, 1, 2, 73, 84, 91, 225, 240, 624, 226, 416, 619, 3, 4, 5, 6, 221, 224, 236, 365, 505, 599, 621, 635, 183, 334, 544, 695, 8, 186, 237, 235, 596, 537, 868, 918, 940, 1009, 1010)
    AND master_category_id != 802
    AND order_media NOT IN ('B2B', 'Bondhu')
GROUP BY order_unique_id) D

GROUP BY order_unique_id
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
        safe_filename = f"FIN_Data_{month_name}_{year}.xlsx"


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
