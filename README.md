# Retail Sales Analytics — AI-Assisted SQL App

A Streamlit application that turns plain-English retail questions into reviewable PostgreSQL queries. The repository also contains the ETL script that normalizes the supplied retail file into six relational tables before the app queries it.

## What this project demonstrates

- Loads a 45 MB tab-delimited source file and expands its nested order fields into **621,806 order-detail rows**.
- Builds a normalized PostgreSQL schema for regions, countries, customers, product categories, products, and orders.
- Uses OpenAI `gpt-4o-mini` to translate a business question into PostgreSQL.
- Keeps a human in the loop: generated SQL is displayed and editable before execution.
- Protects the Streamlit interface with a bcrypt password hash and stores credentials outside source control.

## Architecture

```text
data.csv
   │
   ▼
Python ETL ──► PostgreSQL (6 normalized tables)
                       ▲
                       │ reviewed SQL
User question ──► OpenAI API ──► Streamlit ──► query result
```

## Data model

| Table | Purpose | Key relationship |
| --- | --- | --- |
| `Region` | Sales regions | Parent of `Country` |
| `Country` | Countries in each region | `RegionID → Region` |
| `Customer` | Customer identity and location | `CountryID → Country` |
| `ProductCategory` | Product taxonomy | Parent of `Product` |
| `Product` | Product name and unit price | `ProductCategoryID → ProductCategory` |
| `OrderDetail` | Order date, quantity, customer, and product | Links `Customer` and `Product` |

The source file has one logical row per customer. Product, quantity, and order-date values are stored as semicolon-delimited arrays; `miniproject2_xiaowei.py` expands them into individual order records.

## Quick start

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure PostgreSQL for the ETL

Create a local `.env` file (already ignored by Git):

```dotenv
POSTGRES_USERNAME=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_SERVER=localhost:5432
POSTGRES_DATABASE=retail_analytics
```

Build and populate the normalized schema:

```bash
python miniproject2_xiaowei.py
```

The loader recreates its six target tables, so run it only against the intended development database.

### 3. Configure Streamlit secrets

Generate a bcrypt password hash:

```bash
python generate_password.py
```

Create `.streamlit/secrets.toml`:

```toml
OPENAI_API_KEY = "your_openai_api_key"
HASHED_PASSWORD = "paste_the_generated_bcrypt_hash"

POSTGRES_USERNAME = "your_user"
POSTGRES_PASSWORD = "your_password"
POSTGRES_SERVER = "localhost:5432"
POSTGRES_DATABASE = "retail_analytics"
```

### 4. Run the app

```bash
streamlit run streamlit_app.py
```

Open <http://localhost:8501>, sign in, enter a business question, review the generated SQL, and then choose whether to run it.

## Example questions

- How many customers are in each region?
- Rank countries within each region by total quantity ordered.
- Which product categories have the highest average unit price?
- Show monthly order volume for the five most frequently ordered products.

## Repository layout

```text
.
├── data.csv                    # Source data included with the project
├── miniproject2_xiaowei.py     # PostgreSQL schema creation and ETL
├── streamlit_app.py            # Authenticated natural-language SQL UI
├── generate_password.py        # Local bcrypt hash generator
├── test_render_database.py     # Manual PostgreSQL connectivity check
├── utils.py                    # Environment-based database URL helper
└── requirements.txt
```

## Security and limitations

- Never commit `.env` or `.streamlit/secrets.toml`; both are ignored.
- The model can produce incorrect SQL. Review every query before running it.
- Use a read-only PostgreSQL account for the Streamlit app in any shared deployment.
- The included connectivity script is a manual check, not an automated test suite.
- The application is a portfolio/demo system and does not implement row-level authorization, rate limiting, or a SQL parser/sandbox.
