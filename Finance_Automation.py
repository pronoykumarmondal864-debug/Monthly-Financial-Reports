#!/usr/bin/env python3
import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
import asyncio
from telegram import Bot
import warnings
import os
import tempfile
import sys

warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

# ===============================
#  Environment Credentials
# ===============================
HOST = "sheba-xyz-prod-replica.clwkqg26yift.ap-southeast-1.rds.amazonaws.com"
USER = "pronoy_rdusr"
PORT = 3306
DATABASE = "sheba"
PASSWORD = os.environ.get("DB_PASSWORD")

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

if not all([PASSWORD, BOT_TOKEN, CHAT_ID]):
    print("Missing one of the required environment variables: DB_PASSWORD, BOT_TOKEN, CHAT_ID")
    sys.exit(1)

try:
    CHAT_ID = int(CHAT_ID)
except ValueError:
    print("CHAT_ID must be an integer (negative for groups)")
    sys.exit(1)

# ===============================
#   Date Setup
# ===============================
prev_month_date = datetime.now().replace(day=1) - timedelta(days=1)
month_name = prev_month_date.strftime("%B")
year = prev_month_date.strftime("%Y")

# ===============================
#   SQL QUERIES
# ===============================

# Collection Method with Collected By
QUERY_COLLECTION_METHOD = """
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

# FIN Data Report
QUERY_FIN_DATA = """
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

# Sheba Credit Disbursement
QUERY_SHEBA_CREDIT = """
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

# ===============================
#   Telegram Sender
# ===============================
async def send_to_telegram(file_path: str):
    bot = Bot(token=BOT_TOKEN)
    with open(file_path, "rb") as f:
        await bot.send_document(chat_id=CHAT_ID, document=f, filename=os.path.basename(file_path))
    print(f"Sent {os.path.basename(file_path)} to Telegram")

# ===============================
#   Helper: Run Query + Export
# ===============================
def run_query_and_export(conn, query, filename_prefix):
    df = pd.read_sql(query, conn)
    print(f"Fetched {len(df)} rows for {filename_prefix}")

    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.replace(r'[\x00-\x1F]', '', regex=True)

    safe_filename = f"{filename_prefix}_{month_name}_{year}.xlsx"
    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, safe_filename)
    df.to_excel(file_path, index=False)
    print(f"Saved {file_path}")
    return file_path

# ===============================
#   Main
# ===============================
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

        tasks = []
        reports = [
            ("Collection_method_with_collected_by", QUERY_COLLECTION_METHOD),
            ("FIN_Data", QUERY_FIN_DATA),
            ("Sheba_Credit_Disbursement", QUERY_SHEBA_CREDIT)
        ]

        for prefix, query in reports:
            path = run_query_and_export(conn, query, prefix)
            tasks.append(send_to_telegram(path))

        asyncio.run(asyncio.gather(*tasks))

    except mysql.connector.Error as e:
        print("MySQL error:", e)
    except Exception as e:
        print("Unexpected error:", e)
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("Connection closed")
        print("---------------------------------------------------------------")

# ===============================
#   Entry Point
# ===============================
if __name__ == "__main__":
    main()
