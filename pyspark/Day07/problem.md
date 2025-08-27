# 📝 Problem 1: PySpark – Calculate Session Gaps for Users

### **Problem Statement**

You have a PySpark DataFrame with user session information. Each row represents a session start date for a user. Write a PySpark program to **calculate the gap in days between consecutive sessions** for each user.

### **Sample Input** (`user_sessions`)

| user\_id | session\_date |
| -------- | ------------- |
| U1       | 2025-01-01    |
| U1       | 2025-01-05    |
| U1       | 2025-01-10    |
| U2       | 2025-01-02    |
| U2       | 2025-01-04    |

### **Expected Output**

| user\_id | session\_date | days\_since\_last\_session |
| -------- | ------------- | -------------------------- |
| U1       | 2025-01-01    | NULL                       |
| U1       | 2025-01-05    | 4                          |
| U1       | 2025-01-10    | 5                          |
| U2       | 2025-01-02    | NULL                       |
| U2       | 2025-01-04    | 2                          |

---

# 📝 Problem 2: SQL – Find Customers with Only One Purchase

### **Problem Statement**

You have a SQL table `purchases(customer_id, purchase_date, amount)` representing customer purchases. Write a SQL query to **find all customers who made exactly one purchase**.

### **Sample Input** (`purchases`)

| customer\_id | purchase\_date | amount |
| ------------ | -------------- | ------ |
| C1           | 2025-01-01     | 200    |
| C1           | 2025-01-10     | 300    |
| C2           | 2025-01-05     | 150    |
| C3           | 2025-01-02     | 100    |
| C3           | 2025-01-04     | 200    |

### **Expected Output**

| customer\_id |
| ------------ |
| C2           |

---