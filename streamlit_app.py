import streamlit as st
import pandas as pd
import io

st.set_page_config(layout="wide")
st.title("Temperature and Tide Data Merger")
st.header("Upload your Excel files to begin the merging process.")

st.markdown("""
This application merges tide data with temperature data.

**How it works:**
For each tide entry (specifically, 'Preia-Mar' entries), the application finds the *nearest* temperature reading from each of your temperature data sheets within a **1-hour tolerance**. This means if a temperature reading is more than 1 hour away from a tide entry, it will not be merged.

""")



st.markdown("""
Please follow these steps:

1.  **Upload Tide Data File:**
    *   Please upload an Excel file containing tide information.
    *   **Expected Content:** The file should have a single sheet (the first sheet will be used).
    *   **Expected Columns:**
        *   `Data`: Date (e.g., `YYYY-MM-DD`)
        *   `Hora`: Time (e.g., `HH:MM:SS`)
        *   `Mare`: String, containing values like `Preia-Mar` (only rows with `Preia-Mar` will be processed).
        *   `Alt`: Numeric (e.g., `float`)

2.  **Upload Temperature Data File:**
    *   Please upload an Excel file containing temperature readings from various locations.
    *   **Expected Content:** The file can have multiple sheets, each representing a different location or sensor.
    *   **Expected Columns (per sheet):**
        *   `Date`: Date (e.g., `YYYY-MM-DD`)
        *   `time`: Time (e.g., `HH:MM:SS`)
        *   At least one numeric column representing temperature data (e.g., `CaboSines`, `ALL_TQE_fora`).
        *   The `ficheiro.origem` column will be automatically discarded if present.

3.  Once both files are uploaded, the merging process will begin automatically.
4.  Download the resulting Excel file, which will contain a separate sheet for each temperature data source, merged with the nearest tide data.
""")

@st.cache_data
def process_and_merge_data(tide_file, temp_file):
    """Processes and merges tide and temperature data."""
    
    # --- Read and prepare Mares data (Tide File) ---
    try:
        df_mares = pd.read_excel(tide_file) # Reads the first sheet by default
        st.write("Tide Data loaded from the first sheet.")

        # Filter for 'Preia-Mar' in the 'Mare' column
        df_mares = df_mares[df_mares['Mare'] == 'Preia-Mar']

        # Combine 'Data' and 'Hora' into a single datetime column
        df_mares['Hora'] = df_mares['Hora'].astype(str)
        df_mares['Mares_DateTime'] = pd.to_datetime(df_mares['Data'].astype(str) + ' ' + df_mares['Hora'], errors='coerce')
        df_mares.dropna(subset=['Mares_DateTime'], inplace=True)
        df_mares.sort_values(by='Mares_DateTime', inplace=True)
        df_mares.rename(columns={'Alt': 'Mares_Alt', 'Mare': 'Mares_Mare'}, inplace=True)

    except Exception as e:
        st.error(f"Error processing Tide Data. Please ensure the file format and column names are correct: {e}")
        st.stop() # Stop execution if tide file processing fails

    # --- Read and prepare Temperature data (Temp File) ---
    processed_temp_dfs = {}
    try:
        xls_temp = pd.ExcelFile(temp_file)
        temp_sheet_names = xls_temp.sheet_names

        for sheet_name in temp_sheet_names:
            df_temp_sheet = pd.read_excel(temp_file, sheet_name=sheet_name)

            # Discard 'ficheiro.origem' column if it exists
            if 'ficheiro.origem' in df_temp_sheet.columns:
                df_temp_sheet.drop(columns=['ficheiro.origem'], inplace=True)

            # Identify and remove duplicates
            initial_rows = df_temp_sheet.shape[0]
            df_temp_sheet.drop_duplicates(inplace=True)
            duplicates_removed = initial_rows - df_temp_sheet.shape[0]
            if duplicates_removed > 0:
                st.warning(f"Removed {duplicates_removed} duplicate records from sheet '{sheet_name}'.")

            # Check if the sheet is empty or doesn't have expected columns after duplicate removal
            if df_temp_sheet.empty or 'Date' not in df_temp_sheet.columns or 'time' not in df_temp_sheet.columns:
                st.info(f"Skipping sheet '{sheet_name}' as it is empty or missing 'Date' or 'time' columns after duplicate removal.")
                continue

            # Combine 'Date' and 'time' into a single datetime column
            df_temp_sheet['time'] = df_temp_sheet['time'].astype(str)
            df_temp_sheet['Temp_DateTime'] = pd.to_datetime(df_temp_sheet['Date'].astype(str) + ' ' + df_temp_sheet['time'], errors='coerce')
            df_temp_sheet.dropna(subset=['Temp_DateTime'], inplace=True)
            df_temp_sheet.sort_values(by='Temp_DateTime', inplace=True)

            processed_temp_dfs[sheet_name] = df_temp_sheet

    except Exception as e:
        st.error(f"Error processing Temperature Data. Please ensure the file format and column names are correct: {e}")
        st.stop() # Stop execution if temp file processing fails

    if not processed_temp_dfs:
        st.warning("No valid temperature data sheets found after processing.")
        st.stop()

    # --- Merging Logic ---
    merged_results = {}
    st.subheader("Merging Data...")

    for sheet_name, df_temp_sheet in processed_temp_dfs.items():
        st.write(f"Merging with temperature sheet: {sheet_name}")
        merged_sheet_df = pd.merge_asof(
            df_mares,
            df_temp_sheet,
            left_on='Mares_DateTime',
            right_on='Temp_DateTime',
            direction='nearest',
            tolerance=pd.Timedelta('1 hour')
        )
        merged_sheet_df['Source_Temp_Sheet'] = sheet_name
        merged_results[sheet_name] = merged_sheet_df
        st.write(f"Merged data for {sheet_name}: {merged_sheet_df.shape[0]} rows.")

    if not merged_results:
        st.warning("No data was merged. Please check your input files and processing steps.")
        st.stop()

    return merged_results

tide_file = st.file_uploader("Upload Tide Data File", type=["xlsx"])
temp_file = st.file_uploader("Upload Temperature Data File", type=["xlsx"])

if tide_file and temp_file:
    st.success("Both files uploaded successfully! Proceeding to data processing.")

    with st.spinner("Processing data..."):
        merged_data = process_and_merge_data(tide_file, temp_file)

    # --- Download Option ---
    st.subheader("Download Merged Data")

    # Create an in-memory Excel file
    output_excel_file = io.BytesIO()
    with pd.ExcelWriter(output_excel_file, engine='xlsxwriter') as writer:
        for sheet_name, df in merged_data.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    output_excel_file.seek(0) # Rewind the buffer

    st.download_button(
        label="Download Merged Data as Excel",
        data=output_excel_file,
        file_name="merged_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("Please upload both the Tide Data and Temperature Data Excel files to proceed.")