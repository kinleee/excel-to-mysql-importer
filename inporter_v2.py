import os
import re
import pandas as pd
import mysql.connector
import logging

# Logging qurulması
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# MySQL bağlantısı
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root123321",
            database=None
        )
        logging.info("MySQL bağlantısı uğurla quruldu.")
        return conn
    except mysql.connector.Error as err:
        logging.error(f"MySQL bağlantı xətası: {err}")
        raise

# Adları təmizləmə (database, table, column)
def clean_name(name):
    return re.sub(r'\W|^(?=\d)', '_', str(name))

# Pandas data tiplərini SQL data tiplərinə çevirmə
def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:
        return "TEXT"

# Sütun adlarını yoxlama və avtomatik adlandırma
def ensure_column_names(df):
    new_columns = []
    for idx, col in enumerate(df.columns):
        if pd.isna(col) or str(col).strip() == '' or str(col).startswith('Unnamed'):
            new_col_name = f'Unnamed{idx + 1}'
            logging.info(f"Sütun adı boş və ya 'Unnamed', yeni ad verilir: {new_col_name}")
            new_columns.append(new_col_name)
        else:
            new_columns.append(str(col))
    df.columns = new_columns
    return df

# Excel-də başlıq olub olmadığını yoxlamaq
def has_header(file_path, sheet_name):
    # Excel-in ilk sətrini yoxlayırıq
    df_sample = pd.read_excel(file_path, sheet_name=sheet_name, nrows=1, header=None)
    # Əgər bütün hüceyrələr NaN deyilsə, demək məlumatdır, başlıq yoxdur
    if df_sample.isnull().all(axis=1).iloc[0]:
        return True
    return False

# Schema və cədvəllərin yaradılması
def create_schema_and_tables(cursor, db_name, file_path):
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        cursor.execute(f"USE `{db_name}`")
        logging.info(f"Verilənlər bazası yaradıldı və istifadə edilir: {db_name}")

        excel_file = pd.ExcelFile(file_path)

        for sheet_name in excel_file.sheet_names:
            logging.info(f"{sheet_name} sheet'i işlənir...")

            # Başlıq yoxdursa, header=None oxuyuruq
            if has_header(file_path, sheet_name):
                df = pd.read_excel(file_path, sheet_name=sheet_name, index_col=None)
            else:
                df = pd.read_excel(file_path, sheet_name=sheet_name, index_col=None, header=None)

            # Sütun adlarını düzəlt
            df.columns = df.columns.astype(str)
            df = ensure_column_names(df)

            if df.empty:
                logging.warning(f"{sheet_name} boş olduğu üçün cədvəl yaradılmadı.")
                continue

            clean_sheet_name = clean_name(sheet_name)

            # Cədvəl yaratma
            create_table_query = f"CREATE TABLE IF NOT EXISTS `{clean_sheet_name}` ("
            for col in df.columns:
                clean_col = clean_name(col)
                sql_type = map_dtype(df[col].dtype)
                create_table_query += f"`{clean_col}` {sql_type}, "
            create_table_query = create_table_query.rstrip(", ") + ")"

            cursor.execute(create_table_query)
            logging.info(f"{clean_sheet_name} cədvəli yaradıldı.")

            # Veriləri əlavə et
            for _, row in df.iterrows():
                columns = ', '.join(f"`{clean_name(col)}`" for col in df.columns)
                placeholders = ', '.join(["%s"] * len(df.columns))
                insert_query = f"INSERT INTO `{clean_sheet_name}` ({columns}) VALUES ({placeholders})"
                values = tuple(None if pd.isna(val) else val for val in row)
                cursor.execute(insert_query, values)

            logging.info(f"{clean_sheet_name} cədvəlinə məlumatlar əlavə olundu.")

    except mysql.connector.Error as err:
        logging.error(f"MySQL xətası: {err}")
        raise
    except Exception as e:
        logging.error(f"Xəta baş verdi: {e}")
        raise

# Ana funksiya
def main():
    base_path = "C:/Users/ilkin.ha/Desktop/gis/data"

    try:
        conn = connect_to_mysql()
        cursor = conn.cursor()

        for filename in os.listdir(base_path):
            if filename.endswith((".xlsx", ".xls")):
                file_path = os.path.join(base_path, filename)
                db_name = clean_name(os.path.splitext(filename)[0])
                logging.info(f"{filename} faylı işlənir...")

                create_schema_and_tables(cursor, db_name, file_path)

                conn.commit()
                logging.info(f"{filename} faylı uğurla tamamlandı.")

    except Exception as e:
        logging.error(f"Proqramda xəta baş verdi: {e}")

    finally:
        try:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()
            logging.info("Bağlantılar bağlandı.")
        except Exception as close_error:
            logging.error(f"Bağlantıları bağlayarkən xəta: {close_error}")

if __name__ == "__main__":
    main()
