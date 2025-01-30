# MongoDB Aggregation Pipelines Examples in Python

This repository demonstrates practical use cases of MongoDB Aggregation Pipelines using the `sample_mflix` database. The code includes examples of filtering, grouping, joining collections, and analyzing data with Python and PyMongo.

---

## 📋 Prerequisites
1. **MongoDB Atlas Cluster**:
   - Create a free cluster on [MongoDB Atlas](https://www.mongodb.com/atlas).
   - Load the `sample_mflix` dataset (available in Atlas under "Sample Data").

2. **Python 3.8+**:
   - Ensure Python is installed.

---

## 🛠️ Setup

### 1. Clone the Repository
```bash
git clone https://github.com/denisgavar/mongodb-aggregations.git
cd mongodb-aggregations
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Create a .env file in the root directory with your MongoDB Atlas credentials:

```plaintext
USER=your-atlas-username
PASSWORD=your-atlas-password
HOST=your-cluster-hostname
```
Replace your-cluster-hostname with your Atlas cluster URL (e.g., cluster0.mongodb.net).

## 🚀 Running the Code
Execute the script to run all aggregation examples:

```bash
python aggregations.py
```

### Happy Aggregating! 🚀
