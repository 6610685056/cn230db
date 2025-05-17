import requests
import sqlite3
from tabulate import tabulate
import matplotlib.pyplot as plt

def fetch_data():
    """Fetch product data from FakeStoreAPI"""
    url = 'https://fakestoreapi.com/products'
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Failed to fetch data: {e}")
        return []

def create_database(conn):
    """Create SQLite tables: products, categories, ratings"""
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
    """Insert product and rating data into the database"""
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
    """Perform and print various data analysis results"""
    c = conn.cursor()

    # 1. Average price per category
    print("\n📊 [1] Average Price and Product Count per Category:")
    result = c.execute('''
        SELECT cat.name, COUNT(p.id), ROUND(AVG(p.price), 2)
        FROM products p
        JOIN categories cat ON p.category_id = cat.id
        GROUP BY cat.id
    ''').fetchall()
    print(tabulate(result, headers=["Category", "Total Products", "Average Price"], tablefmt="pretty"))

    # 2. Top category by total sales
    print("\n🏆 [2] Top Category by Total Sales:")
    result = c.execute('''
        SELECT cat.name, SUM(r.count)
        FROM ratings r
        JOIN products p ON r.product_id = p.id
        JOIN categories cat ON p.category_id = cat.id
        GROUP BY cat.id
        ORDER BY SUM(r.count) DESC LIMIT 1
    ''').fetchone()
    print(f"Category: {result[0]} | Total Sold: {result[1]}")

    # 3. Top 5 selling products
    print("\n🔥 [3] Top 5 Best Selling Products:")
    result = c.execute('''
        SELECT p.title, r.count
        FROM ratings r
        JOIN products p ON r.product_id = p.id
        ORDER BY r.count DESC
        LIMIT 5
    ''').fetchall()
    print(tabulate(result, headers=["Product Title", "Sold Count"], tablefmt="pretty"))

    # 4. Products with higher-than-average ratings
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
    """Generate bar, pie, and horizontal bar graphs from data"""
    c = conn.cursor()

    # 1. Products per category
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
    plt.bar(categories, counts)
    plt.title("Number of Products per Category")
    plt.tight_layout()
    plt.savefig("bar_products_per_category.png")
    plt.show()

    # 2. Sales distribution
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
    plt.pie(sizes, labels=labels, autopct='%1.1f%%')
    plt.title("Sales Distribution by Category")
    plt.axis('equal')
    plt.savefig("pie_sales_by_category.png")
    plt.show()

    # 3. Top 5 selling products
    c.execute('''
        SELECT p.title, r.count
        FROM ratings r
        JOIN products p ON r.product_id = p.id
        ORDER BY r.count DESC LIMIT 5
    ''')
    data = c.fetchall()
    titles = [row[0][:20] + "..." if len(row[0]) > 20 else row[0] for row in data]
    counts = [row[1] for row in data]

    plt.figure(figsize=(10, 5))
    plt.barh(titles, counts)
    plt.title("Top 5 Best Selling Products")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig("bar_top5_products.png")
    plt.show()

def main():
    data = fetch_data()
    if not data:
        print("No data to process.")
        return
    conn = sqlite3.connect("fakestore_advanced.db")
    create_database(conn)
    insert_data(conn, data)
    analytics(conn)
    plot_graphs(conn)
    conn.close()

if __name__ == '__main__':
    main()
