# 🏦 Spark Banking Data Engineering Project

## 📖 Problem Statement
Banks generate large volumes of transactional data. The goal of this project is to:
- Aggregate account balances
- Join with customer data
- Generate meaningful insights for reporting

---

## 📂 Input Data

### 1. balanceAccount.csv
Contains account-level transaction data:
- account_no
- product description
- fiscal year & period
- amount

### 2. customerAccount.csv
Contains customer mapping:
- customer_id
- source_account_identifier

---

## ⚙️ Transformation Logic

- Read CSV data using Spark
- Clean and rename columns
- Aggregate total balance using `groupBy`
- Join with customer data using account number
- Handle null values

---

## 📤 Output

Example output:

stg_id,customer_id,source_account_identifier,effectivate_start_date,effectivate_end_date,customer_system_product_code,product_account_class_code,primary_customer_flag,relationship_start_date,legal_entity_identifier,trade_entity_identifier,relationship_end_date,source_system_id,business_date,source_acount_identifier
0,742304030,PC-128350,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1,12/31/2025,NULL
0,742304030,PC-128350,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1,12/31/2025,NULL
0,742304030,PC-128350,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1,12/31/2025,NULL
0,742304030,PC-128350,NULL,NULL,NULL,NULL,NULL,NULL	NULL,NULL,NULL,1,12/31/2025,NULL

<img width="1710" height="149" alt="image" src="https://github.com/user-attachments/assets/585289bc-f6a3-4e38-8542-0ae6b4597ecf" />

---

## 🛠 Tools & Technologies

- Apache Spark
- Scala
- IntelliJ IDEA
- GitHub

---

## ▶️ How to Run

1. Open project in IntelliJ
2. Run `Main.scala`

---

## 👨‍💻 Author

Sachin N R
Aspiring Data Engineer
