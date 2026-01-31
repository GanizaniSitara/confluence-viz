# description: Seeds data for Confluence visualization.

import argparse
import json
import os
import random
import string
import sys
import time
from pathlib import Path

import requests

from config_loader import load_confluence_settings

# ---------------------------------------------------------------------------
# SQL Content Generators for Testing
# ---------------------------------------------------------------------------

# Oracle SQL snippets
ORACLE_SQL_SNIPPETS = [
    # Basic queries
    "SELECT employee_id, first_name, last_name, salary FROM employees WHERE department_id = 10;",
    "SELECT * FROM orders WHERE order_date BETWEEN TO_DATE('2024-01-01', 'YYYY-MM-DD') AND SYSDATE;",
    "SELECT department_name, COUNT(*) as emp_count FROM employees e JOIN departments d ON e.department_id = d.department_id GROUP BY department_name;",
    # PL/SQL blocks
    """DECLARE
    v_total NUMBER := 0;
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM customers WHERE status = 'ACTIVE';
    DBMS_OUTPUT.PUT_LINE('Active customers: ' || v_count);
END;""",
    # DDL statements
    """CREATE TABLE audit_log (
    log_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    action_type VARCHAR2(50) NOT NULL,
    table_name VARCHAR2(100),
    old_value CLOB,
    new_value CLOB,
    changed_by VARCHAR2(100) DEFAULT USER,
    changed_at TIMESTAMP DEFAULT SYSTIMESTAMP
);""",
    # Complex queries
    """SELECT
    p.product_name,
    c.category_name,
    SUM(oi.quantity) as total_sold,
    ROUND(AVG(oi.unit_price), 2) as avg_price
FROM products p
INNER JOIN categories c ON p.category_id = c.category_id
LEFT JOIN order_items oi ON p.product_id = oi.product_id
WHERE p.discontinued = 0
GROUP BY p.product_name, c.category_name
HAVING SUM(oi.quantity) > 100
ORDER BY total_sold DESC
FETCH FIRST 10 ROWS ONLY;""",
    # Stored procedure
    """CREATE OR REPLACE PROCEDURE update_salary (
    p_emp_id IN NUMBER,
    p_percent IN NUMBER
) AS
    v_old_salary NUMBER;
    v_new_salary NUMBER;
BEGIN
    SELECT salary INTO v_old_salary FROM employees WHERE employee_id = p_emp_id;
    v_new_salary := v_old_salary * (1 + p_percent/100);
    UPDATE employees SET salary = v_new_salary WHERE employee_id = p_emp_id;
    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR(-20001, 'Employee not found');
END;""",
    # Analytical functions
    """SELECT
    employee_id,
    department_id,
    salary,
    RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) as salary_rank,
    LAG(salary) OVER (PARTITION BY department_id ORDER BY salary) as prev_salary,
    ROUND(salary / SUM(salary) OVER (PARTITION BY department_id) * 100, 2) as pct_of_dept
FROM employees
WHERE hire_date > ADD_MONTHS(SYSDATE, -12);""",
]

# MS SQL Server snippets
MSSQL_SNIPPETS = [
    # Basic queries
    "SELECT TOP 100 CustomerID, CompanyName, ContactName FROM Customers WHERE Country = 'USA' ORDER BY CompanyName;",
    "SELECT * FROM Orders WHERE OrderDate >= DATEADD(month, -3, GETDATE());",
    "SELECT ProductName, UnitsInStock, UnitPrice FROM Products WHERE Discontinued = 0 AND UnitsInStock < ReorderLevel;",
    # T-SQL blocks
    """DECLARE @TotalRevenue DECIMAL(18,2);
DECLARE @OrderCount INT;

SELECT @TotalRevenue = SUM(Quantity * UnitPrice),
       @OrderCount = COUNT(DISTINCT OrderID)
FROM [Order Details];

PRINT 'Total Revenue: $' + CAST(@TotalRevenue AS VARCHAR(20));
PRINT 'Order Count: ' + CAST(@OrderCount AS VARCHAR(10));""",
    # DDL with constraints
    """CREATE TABLE EmployeeHistory (
    HistoryID INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT NOT NULL,
    ActionType NVARCHAR(50) NOT NULL,
    OldDepartment NVARCHAR(100),
    NewDepartment NVARCHAR(100),
    OldSalary DECIMAL(10,2),
    NewSalary DECIMAL(10,2),
    ChangedBy NVARCHAR(100) DEFAULT SUSER_SNAME(),
    ChangedAt DATETIME2 DEFAULT SYSDATETIME(),
    CONSTRAINT FK_Employee FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
);""",
    # CTE and window functions
    """WITH SalesRanking AS (
    SELECT
        s.SalesPersonID,
        e.FirstName + ' ' + e.LastName AS SalesPersonName,
        SUM(soh.TotalDue) AS TotalSales,
        ROW_NUMBER() OVER (ORDER BY SUM(soh.TotalDue) DESC) AS SalesRank
    FROM Sales.SalesOrderHeader soh
    INNER JOIN Sales.SalesPerson s ON soh.SalesPersonID = s.BusinessEntityID
    INNER JOIN HumanResources.Employee e ON s.BusinessEntityID = e.BusinessEntityID
    WHERE soh.OrderDate >= DATEADD(year, -1, GETDATE())
    GROUP BY s.SalesPersonID, e.FirstName, e.LastName
)
SELECT * FROM SalesRanking WHERE SalesRank <= 10;""",
    # Stored procedure with error handling
    """CREATE PROCEDURE usp_TransferFunds
    @FromAccount INT,
    @ToAccount INT,
    @Amount DECIMAL(18,2)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;

        UPDATE Accounts SET Balance = Balance - @Amount WHERE AccountID = @FromAccount;
        UPDATE Accounts SET Balance = Balance + @Amount WHERE AccountID = @ToAccount;

        INSERT INTO TransactionLog (FromAccount, ToAccount, Amount, TransactionDate)
        VALUES (@FromAccount, @ToAccount, @Amount, GETDATE());

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;""",
    # Pivot query
    """SELECT *
FROM (
    SELECT
        YEAR(OrderDate) AS OrderYear,
        MONTH(OrderDate) AS OrderMonth,
        TotalDue
    FROM Sales.SalesOrderHeader
    WHERE OrderDate >= '2023-01-01'
) AS SourceTable
PIVOT (
    SUM(TotalDue)
    FOR OrderMonth IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12])
) AS PivotTable
ORDER BY OrderYear;""",
    # Merge statement
    """MERGE INTO TargetProducts AS target
USING SourceProducts AS source
ON target.ProductID = source.ProductID
WHEN MATCHED AND target.Price <> source.Price THEN
    UPDATE SET target.Price = source.Price, target.LastUpdated = GETDATE()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ProductID, ProductName, Price, LastUpdated)
    VALUES (source.ProductID, source.ProductName, source.Price, GETDATE())
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;""",
]

def generate_sql_content():
    """Generate random SQL content for insertion into pages as plain floating text."""
    # Pick random SQL type (Oracle or MS SQL)
    sql_type = random.choice(['oracle', 'mssql'])
    snippets = ORACLE_SQL_SNIPPETS if sql_type == 'oracle' else MSSQL_SNIPPETS
    sql_snippet = random.choice(snippets)

    # Context phrases that might appear before/after SQL in real Confluence pages
    context_before = [
        "Here is the query we discussed:",
        "Use this SQL:",
        "The query is:",
        "SQL statement:",
        "Database query:",
        "Run this:",
        "Execute the following:",
        "Current query:",
        "",  # Sometimes no intro text at all
        "",
        "",
    ]

    context_after = [
        "Let me know if you have questions.",
        "This runs on the production database.",
        "Make sure to test first.",
        "Contact DBA team for access.",
        "",  # Often no closing text
        "",
        "",
        "",
    ]

    before = random.choice(context_before)
    after = random.choice(context_after)

    # Build plain text content - just paragraphs with SQL pasted in
    parts = []
    if before:
        parts.append(f"<p>{before}</p>")
    # SQL as plain paragraph text (no pre, no code tags)
    # Replace newlines with <br/> to preserve formatting in plain text
    sql_as_text = sql_snippet.replace('\n', '<br/>')
    parts.append(f"<p>{sql_as_text}</p>")
    if after:
        parts.append(f"<p>{after}</p>")

    return "\n".join(parts)


def should_insert_sql(page_index):
    """Determine if SQL should be inserted (roughly every 3rd or 4th page)."""
    # Random chance of 25-33% (every 3rd to 4th page on average)
    return random.random() < 0.30


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rand_key(existing_keys, length=5):
    while True:
        key = "".join(random.choices(string.ascii_uppercase, k=length))
        if key not in existing_keys:
            return key

def read_seeds(path: Path):
    if not path.exists():
        return ["Enterprise", "Application", "Management", "Operations"]
    if path.suffix.lower() == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

def sleep_with_backoff(attempt):
    time.sleep(min(2 ** attempt, 60))

def post_with_retry(url, payload, auth, verify, headers=None):
    attempt = 0
    while True:
        resp = requests.post(url, json=payload, auth=auth, verify=verify, headers=headers)
        if resp.status_code == 429:
            attempt += 1
            sleep_with_backoff(attempt)
            continue
        return resp

# ---------------------------------------------------------------------------
# Core Functions
# ---------------------------------------------------------------------------

def create_space(base_url, auth, verify, key, name, desc=""):
    url = f"{base_url}/rest/api/space"
    payload = {
        "key": key,
        "name": name,
        "description": {
            "plain": {
                "value": desc or name,
                "representation": "plain",
            }
        },
    }
    r = post_with_retry(url, payload, auth, verify)
    return r.ok

def generate_content_with_corporate_lorem(paragraphs=2):
    """
    Generate inventive content using the CorporateLorem API.
    The API endpoint is:
    http://corporatelorem.kovah.de/api/[amount of paragraphs]?format=text
    Appending format=text returns plain text; if paragraph tags are needed,
    the API can be modified with an additional query parameter.
    """
    try:
        url = f"http://corporatelorem.kovah.de/api/{paragraphs}?format=text"
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        else:
            print(f"Error getting CorporateLorem content: status {response.status_code}", file=sys.stderr)
            return "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
    except Exception as e:
        print(f"Error generating content from CorporateLorem API: {e}", file=sys.stderr)
        return "Lorem ipsum dolor sit amet, consectetur adipiscing elit."

def create_page(base_url, auth, verify, space_key, title, content=None, use_ollama=False, raw_html=False):
    """
    Create a page in Confluence.
    If use_ollama is True and no content is provided, generate content using CorporateLorem API.
    If raw_html is True, use content as-is without wrapping in paragraph tags.
    """
    if use_ollama and content is None:
        content = generate_content_with_corporate_lorem()
    newline_char = '\n'
    # Wrap plain text content in paragraph tags (replacing newlines), unless raw_html is True
    if raw_html:
        html_content = content
    elif use_ollama:
        html_content = f"<p>{content.replace(newline_char, '</p><p>')}</p>"
    else:
        html_content = f"<p>{content}</p>"
    payload = {
        "type": "page",
        "title": title,
        "space": {"key": space_key},
        "body": {
            "storage": {
                "value": html_content,
                "representation": "storage",
            }
        },
    }
    r = post_with_retry(f"{base_url}/rest/api/content", payload, auth, verify)
    return r.ok

# ---------------------------------------------------------------------------
# Main script
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="Confluence base URL (overrides settings.ini)")
    parser.add_argument("--user", help="Username (overrides settings.ini)")
    parser.add_argument("--password", help="Password (overrides settings.ini)")
    parser.add_argument("--spaces", type=int, help="Number of spaces to create")
    parser.add_argument("--min-pages", type=int, help="Minimum pages per space")
    parser.add_argument("--max-pages", type=int, help="Maximum pages per space")
    parser.add_argument("--seed-file", type=Path, help="Seed words file (txt or json)")
    parser.add_argument("--verify-ssl", action="store_true", help="Enable SSL verification (overrides settings.ini)")
    parser.add_argument("--use-ollama", action="store_true", help="Generate content using CorporateLorem API")
    parser.add_argument("--sql", action="store_true", help="Randomly insert SQL content (Oracle/MS SQL) into ~1/3 of pages")
    args = parser.parse_args()

    # Load settings from settings.ini
    try:
        conf_settings = load_confluence_settings()
        print("Loaded Confluence settings from settings.ini")
    except FileNotFoundError:
        print("Warning: settings.ini not found, using fallback defaults", file=sys.stderr)
        conf_settings = {
            'base_url': 'http://192.168.65.128:8090',
            'username': 'admin',
            'password': 'admin',
            'verify_ssl': False
        }

    # Default configuration for non-confluence settings
    DEFAULT_SPACES = 300
    DEFAULT_MIN_PAGES = 10
    DEFAULT_MAX_PAGES = 100
    DEFAULT_SEED_FILE = Path("seeds.txt")
    DEFAULT_USE_OLLAMA = True
    DEFAULT_SQL = False

    # Use provided arguments, fallback to settings.ini, then defaults
    url = args.url or conf_settings['base_url']
    user = args.user or conf_settings['username']
    password = args.password or conf_settings['password']
    verify_ssl = args.verify_ssl or conf_settings.get('verify_ssl', False)
    spaces = args.spaces or DEFAULT_SPACES
    min_pages = args.min_pages or DEFAULT_MIN_PAGES
    max_pages = args.max_pages or DEFAULT_MAX_PAGES
    seed_file = args.seed_file or DEFAULT_SEED_FILE
    use_ollama = args.use_ollama or DEFAULT_USE_OLLAMA
    use_sql = args.sql or DEFAULT_SQL

    print(f"Starting content creation with the following settings:")
    print(f"URL: {url}")
    print(f"Spaces: {spaces}")
    print(f"Pages per space: {min_pages}-{max_pages}")
    print(f"Using CorporateLorem API for content: {use_ollama}")
    print(f"SQL content injection enabled: {use_sql}")

    random.seed()
    auth = (user, password)
    base_url = url.rstrip("/")

    if not verify_ssl:
        requests.packages.urllib3.disable_warnings()

    seeds = read_seeds(seed_file)
    print(f"Loaded {len(seeds)} seed words for content generation")
    existing_keys = set()

    for i in range(spaces):
        key = rand_key(existing_keys)
        existing_keys.add(key)
        name = f"{random.choice(seeds)} Space {i + 1}"
        print(f"Creating space [{i + 1}/{spaces}]: {name} (key: {key})...")
        if not create_space(base_url, auth, verify_ssl, key, name):
            print(f"Failed to create space {name} ({key})", file=sys.stderr)
            continue

        page_total = random.randint(min_pages, max_pages)
        print(f"  Creating {page_total} pages in space {key}...")
        sql_pages_count = 0

        for p in range(page_total):
            title = f"{random.choice(seeds)} Page {p + 1}"
            print(f"    Creating page [{p + 1}/{page_total}]: {title}")

            # Determine if this page should include SQL content
            include_sql = use_sql and should_insert_sql(p)
            sql_suffix = ""
            if include_sql:
                sql_suffix = generate_sql_content()
                sql_pages_count += 1
                print(f"      [SQL] Adding SQL content to this page")

            if use_ollama:
                print(f"      Generating content with CorporateLorem API...")
                base_content = generate_content_with_corporate_lorem()
                if include_sql:
                    # Append SQL content after the generated content
                    newline_char = '\n'
                    html_content = f"<p>{base_content.replace(newline_char, '</p><p>')}</p>{sql_suffix}"
                    if not create_page(base_url, auth, verify_ssl, key, title, content=html_content, raw_html=True):
                        print(f"Failed to create page {title} in space {key}", file=sys.stderr)
                    else:
                        print(f"      Page created successfully (with SQL)")
                else:
                    if not create_page(base_url, auth, verify_ssl, key, title, use_ollama=True):
                        print(f"Failed to create page {title} in space {key}", file=sys.stderr)
                    else:
                        print(f"      Page created successfully")
            else:
                content = " ".join(random.choices(seeds, k=30))
                if include_sql:
                    html_content = f"<p>{content}</p>{sql_suffix}"
                    if not create_page(base_url, auth, verify_ssl, key, title, content=html_content, raw_html=True):
                        print(f"Failed to create page {title} in space {key}", file=sys.stderr)
                    else:
                        print(f"      Page created successfully (with SQL)")
                else:
                    print(f"      Using random seed content")
                    if not create_page(base_url, auth, verify_ssl, key, title, content):
                        print(f"Failed to create page {title} in space {key}", file=sys.stderr)
                    else:
                        print(f"      Page created successfully")

        if use_sql:
            print(f"  Space {key} complete: {sql_pages_count}/{page_total} pages contain SQL content")

    print("\nAll content creation completed successfully!")

if __name__ == "__main__":
    main()