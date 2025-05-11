import requests
import sqlite3
from tabulate import tabulate
import matplotlib.pyplot as plt

def fetch_data():
    url = 'https://fakestoreapi.com/products'
    response = requests.get(url)
    return response.json()

def create_database(conn):
    c = conn.cursor()
    c.executescript('''
        DROP TABLE IF EXISTS products;
        DROP TABLE IF EXISTS categories;
        DROP TABLE IF EXISTS ratings;
    ''')
    c.execute('''
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
    ''')
    c.execute('''
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            title TEXT,
            price REAL,
            category_id INTEGER,
            FOREIGN KEY (category_id) REFERENCES categories(id)
        )
    ''')
    c.execute('''
        CREATE TABLE ratings (
            product_id INTEGER PRIMARY KEY,
            rate REAL,
            count INTEGER,
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    ''')
    conn.commit()

def insert_data(conn, products):
    c = conn.cursor()
    for p in products:
        c.execute('INSERT OR IGNORE INTO categories (name) VALUES (?)', (p['category'],))
        c.execute('SELECT id FROM categories WHERE name = ?', (p['category'],))
        cat_id = c.fetchone()[0]

        c.execute('''
            INSERT INTO products (id, title, price, category_id)
            VALUES (?, ?, ?, ?)
        ''', (p['id'], p['title'], p['price'], cat_id))

        c.execute('''
            INSERT INTO ratings (product_id, rate, count)
            VALUES (?, ?, ?)
        ''', (p['id'], p['rating']['rate'], p['rating']['count']))
    conn.commit()

def analytics(conn):
    c = conn.cursor()

    print("\n📊 [1] Average Price and Product Count per Category:")
    result = c.execute('''
        SELECT cat.name AS Category, 
               COUNT(p.id) AS Total_Products, 
               ROUND(AVG(p.price),2) AS Average_Price
        FROM products p
        JOIN categories cat ON p.category_id = cat.id
        GROUP BY cat.id
    ''').fetchall()
    print(tabulate(result, headers=["Category", "Total Products", "Average Price"], tablefmt="pretty"))

    print("\n🏆 [2] Top Category by Total Sales (based on rating count):")
    result = c.execute('''
        SELECT cat.name, SUM(r.count) AS Total_Sold
        FROM ratings r
        JOIN products p ON r.product_id = p.id
        JOIN categories cat ON p.category_id = cat.id
        GROUP BY cat.id
        ORDER BY Total_Sold DESC
        LIMIT 1
    ''').fetchone()
    print(f"Category: {result[0]} | Total Sold: {result[1]}")

    print("\n🔥 [3] Top 5 Best Selling Products:")
    result = c.execute('''
        SELECT p.title, r.count
        FROM ratings r
        JOIN products p ON r.product_id = p.id
        ORDER BY r.count DESC
        LIMIT 5
    ''').fetchall()
    print(tabulate(result, headers=["Product Title", "Sold Count"], tablefmt="pretty"))

    print("\n⭐ [4] Products With Rating Higher Than Category Average:")
    result = c.execute('''
        SELECT p.title, cat.name, r.rate, (
            SELECT AVG(r2.rate)
            FROM ratings r2
            JOIN products p2 ON r2.product_id = p2.id
            WHERE p2.category_id = p.category_id
        ) AS avg_cat_rating
        FROM products p
        JOIN categories cat ON p.category_id = cat.id
        JOIN ratings r ON p.id = r.product_id
        WHERE r.rate > (
            SELECT AVG(r2.rate)
            FROM ratings r2
            JOIN products p2 ON r2.product_id = p2.id
            WHERE p2.category_id = p.category_id
        )
        ORDER BY r.rate DESC
    ''').fetchall()
    print(tabulate(result, headers=["Product", "Category", "Rating", "Category Avg"], tablefmt="pretty"))

def plot_graphs(conn):
    c = conn.cursor()

    c.execute('''
        SELECT cat.name, COUNT(p.id)
        FROM products p
        JOIN categories cat ON p.category_id = cat.id
        GROUP BY cat.id
    ''')
    data = c.fetchall()
    categories = [row[0] for row in data]
    counts = [row[1] for row in data]

    plt.figure(figsize=(8, 5))
    plt.bar(categories, counts, color='skyblue')
    plt.title("Number of Products per Category")
    plt.xlabel("Category")
    plt.ylabel("Product Count")
    plt.tight_layout()
    plt.savefig("bar_products_per_category.png")
    plt.show()

    c.execute('''
        SELECT cat.name, SUM(r.count)
        FROM ratings r
        JOIN products p ON r.product_id = p.id
        JOIN categories cat ON p.category_id = cat.id
        GROUP BY cat.id
    ''')
    data = c.fetchall()
    labels = [row[0] for row in data]
    sizes = [row[1] for row in data]

    plt.figure(figsize=(6, 6))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title("Sales Distribution by Category (Rating Count)")
    plt.axis('equal')
    plt.savefig("pie_sales_by_category.png")
    plt.show()

    c.execute('''
        SELECT p.title, r.count
        FROM ratings r
        JOIN products p ON r.product_id = p.id
        ORDER BY r.count DESC
        LIMIT 5
    ''')
    data = c.fetchall()
    titles = [row[0][:20] + "..." if len(row[0]) > 20 else row[0] for row in data]
    counts = [row[1] for row in data]

    plt.figure(figsize=(10, 5))
    plt.barh(titles, counts, color='orange')
    plt.xlabel("Sold Count")
    plt.title("Top 5 Best Selling Products")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig("bar_top5_products.png")
    plt.show()

def main():
    data = fetch_data()
    conn = sqlite3.connect("fakestore_advanced.db")
    create_database(conn)
    insert_data(conn, data)
    analytics(conn)
    plot_graphs(conn)
    conn.close()

if __name__ == '__main__':
    main()
