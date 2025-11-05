import streamlit as st
import pandas as pd
import json
import os
from io import BytesIO
from sqlalchemy import create_engine
from google.cloud import storage
from google.oauth2 import service_account
import google.generativeai as genai
from google.cloud.sql.connector import Connector
import pymysql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


PROJECT_ID = "ai-data-normalaizer"
BUCKET_NAME = "normalizer-bucket"
SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), "ai-data-normalaizer-6c6d803c3bb1.json")  # Path to downloaded service account key
GEMINI_API_KEY = "AIzaSyC8KdKGi0ec-E5iSX4RdVQ3WGDVWk5HeyI"

# Gemini configuration
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    st.error("GEMINI_API_KEY environment variable not set")

# Load GCP credentials
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)


def upload_to_gcs(file, filename):
    try:
        client = storage.Client(credentials=credentials, project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(filename)
        blob.upload_from_file(file)
        return f"gs://{BUCKET_NAME}/{filename}"
    except Exception as e:
        st.error(f"GCS Upload Failed: {e}")
        return None


def get_sql_connection():
    connector = Connector(credentials=credentials)

    def getconn():
        conn = connector.connect(
            "ai-data-normalaizer:us-central1:ai-normalizer-db",
            "pymysql",
            user="root",
            password="123qwesai@R4"
        )
        return conn

    engine = create_engine("mysql+pymysql://", creator=getconn)
    return engine


def get_available_models():
    """Get list of available Gemini models"""
    try:
        models = genai.list_models()
        return [m.name for m in models if 'generateContent' in m.supported_generation_methods]
    except Exception as e:
        st.error(f"Error listing models: {e}")
        return []

def normalize_data_with_gemini(df, normal_form):
    sample_data = df.head(10).to_json(orient="records")

    prompt = f"""
    You are a database normalization expert.
    Normalize the following dataset into {normal_form}.
    Provide:
    1. Normalized table schemas (with table names, attributes, keys)
    2. SQL CREATE TABLE statements for the normalized design
    3. Explanation for each normalization step

    Data sample:
    {sample_data}
    """

    model = genai.GenerativeModel("models/gemini-2.0-flash")
    response = model.generate_content(prompt)
    return response.text if response else "No response received."


def extract_sql_statements(text):
    """Extract CREATE TABLE statements from Gemini response"""
    lines = text.split('\n')
    sql_statements = []
    in_sql_block = False
    current_statement = []
    
    for line in lines:
        if 'CREATE TABLE' in line.upper():
            in_sql_block = True
            current_statement = [line]
        elif in_sql_block:
            current_statement.append(line)
            if ';' in line:
                sql_statements.append('\n'.join(current_statement))
                current_statement = []
                in_sql_block = False
    
    return '\n\n'.join(sql_statements) if sql_statements else text


def main():
    st.set_page_config(page_title="AI Data Normalizer (GCP)", layout="wide")
    st.title("AI Data Normalizer (GCP Integrated)")
    st.write("Upload your data file, store in GCS, normalize using Gemini AI, and save results in Cloud SQL.")

    uploaded_file = st.file_uploader("Upload your data file", type=["csv", "xlsx", "json"])
    normal_form = st.selectbox("Choose normalization form:", ["1NF", "2NF", "3NF", "BCNF"])

    if uploaded_file:
        # Upload file to GCS
        gcs_path = upload_to_gcs(uploaded_file, uploaded_file.name)
        if gcs_path:
            st.success(f"File uploaded to GCS: {gcs_path}")

        # Read the uploaded file into DataFrame
        uploaded_file.seek(0)
        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith(".xlsx"):
            df = pd.read_excel(uploaded_file)
        elif uploaded_file.name.endswith(".json"):
            df = pd.DataFrame(json.load(uploaded_file))
        else:
            st.error("Unsupported file type.")
            st.stop()

        st.subheader("Original Data")
        st.dataframe(df.head())

        # Show available models (for debugging)
        if st.checkbox("Show available models"):
            models = get_available_models()
            st.write("Available models:", models)

        # Normalize using Gemini
        if st.button("Normalize Data"):
            with st.spinner("AI is normalizing your data..."):
                result = normalize_data_with_gemini(df, normal_form)

            st.subheader("Normalized Output")
            st.write(result)

            # Save schema result to Cloud SQL
            try:
                engine = get_sql_connection()
                with engine.connect() as conn:
                    conn.execute(
                        "CREATE DATABASE IF NOT EXISTS `ai-normalizer-db`"
                    )
                    conn.execute("USE `ai-normalizer-db`")
                    conn.execute(
                        "CREATE TABLE IF NOT EXISTS normalization_results (id INT AUTO_INCREMENT PRIMARY KEY, form VARCHAR(10), schema_output TEXT)"
                    )
                    conn.execute(
                        "INSERT INTO normalization_results (form, schema_output) VALUES (%s, %s)",
                        (normal_form, result)
                    )
                    conn.commit()
            except Exception:
                pass  # Silently handle Cloud SQL errors

            # Allow download of SQL statements
            sql_content = extract_sql_statements(result)
            st.download_button(
                label="Download SQL Schema",
                data=sql_content,
                file_name=f"normalized_schema_{normal_form}.sql",
                mime="text/plain"
            )

if __name__ == "__main__":
    main()
