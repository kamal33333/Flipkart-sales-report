import streamlit as st
import pandas as pd
import os
import re
import io

# ==========================================
# PAGE CONFIGURATION
# ==========================================
st.set_page_config(
    page_title="Flipkart Sales Report Generator",
    page_icon="🛒",
    layout="wide"
)

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def clean_col_names(df):
    """Standardizes column names to strip whitespace."""
    df.columns = [str(c).strip() for c in df.columns]
    return df

def clean_id(val):
    """Normalizes IDs (removes scientific notation, decimals)."""
    if pd.isna(val) or val == '': return ""
    try:
        if isinstance(val, float): return '{:.0f}'.format(val)
        s = str(val).strip()
        if 'e' in s.lower(): return '{:.0f}'.format(float(s))
        return s.split('.')[0]
    except:
        return str(val)

def clean_sku(val):
    """Removes quotes, 'SKU:', and whitespace."""
    if pd.isna(val): return "Unknown_SKU"
    txt = str(val).strip()
    txt = txt.replace('"', '')
    txt = re.sub(r'^sku:\s*', '', txt, flags=re.IGNORECASE)
    return txt.strip()

def load_sheet_robust(file_obj, sheet_name):
    """Loads a sheet by searching for the header row dynamically from an uploaded file."""
    try:
        xl = pd.ExcelFile(file_obj)
        if sheet_name not in xl.sheet_names:
            return None
        
        # Scan first 20 rows to find header
        preview = pd.read_excel(file_obj, sheet_name=sheet_name, header=None, nrows=20)
        header_idx = -1
        for idx, row in preview.iterrows():
            if row.astype(str).str.contains('Order Item ID', case=False, na=False).any():
                header_idx = idx
                break
        
        if header_idx == -1: return None
        return pd.read_excel(file_obj, sheet_name=sheet_name, header=header_idx)
    except Exception as e:
        st.warning(f"Error loading sheet '{sheet_name}': {e}")
        return None

def process_single_month_data(file_obj, filename_for_log):
    """Process a single month's file from a Streamlit UploadedFile object."""
    # 1. Load Data
    sales_df = load_sheet_robust(file_obj, 'Sales Report')
    # Reset file pointer after reading the first sheet
    file_obj.seek(0) 
    cashback_df = load_sheet_robust(file_obj, 'Cash Back Report')
    file_obj.seek(0)
    
    if sales_df is None:
        raise ValueError(f"Sales Report sheet missing or formatted incorrectly in file: {filename_for_log}.")

    # 2. Clean Sales Data
    sales_df = clean_col_names(sales_df)
    
    if 'Order Item ID' not in sales_df.columns:
        raise ValueError(f"Could not find 'Order Item ID' column in Sales Report of {filename_for_log}")
        
    sales_df['Order Item ID'] = sales_df['Order Item ID'].apply(clean_id)
    
    # SKU Cleaning
    if 'SKU' in sales_df.columns:
        sales_df['SKU'] = sales_df['SKU'].apply(clean_sku)
    else:
        sales_df['SKU'] = 'Unknown_SKU'

    # Map SKU to Title
    title_map = {}
    if 'Product Title/Description' in sales_df.columns:
        sales_df['Product Title/Description'] = sales_df['Product Title/Description'].apply(lambda x: str(x).replace('"', '').strip())
        title_map = sales_df.groupby('SKU')['Product Title/Description'].first().to_dict()

    # Ensure Numeric Columns
    qty_col = 'Item Quantity'
    if qty_col not in sales_df.columns:
        qty_col = next((c for c in sales_df.columns if 'quantity' in c.lower()), None)
        if not qty_col:
             raise ValueError(f"Could not find Quantity column in Sales Report of {filename_for_log}")

    val_col = 'Taxable Value (Final Invoice Amount -Taxes)' 
    if val_col not in sales_df.columns:
        possible_cols = [c for c in sales_df.columns if 'Taxable Value' in c]
        if possible_cols:
            val_col = possible_cols[0]
        else:
            st.warning(f"CRITICAL WARNING: Could not find 'Taxable Value' column in {filename_for_log}. Using 0.")
            sales_df['Taxable Value_Dummy'] = 0
            val_col = 'Taxable Value_Dummy'

    sales_df[qty_col] = pd.to_numeric(sales_df[qty_col], errors='coerce').fillna(0)
    sales_df[val_col] = pd.to_numeric(sales_df[val_col], errors='coerce').fillna(0)

    # 3. Categorize Transactions
    type_col = 'Event Sub Type'
    if type_col not in sales_df.columns: 
        type_col = 'Event Type'
    if type_col not in sales_df.columns:
        st.warning(f"Could not find Event Type column in {filename_for_log}. Treating all as sales.")
        sales_df['Event_Type_Dummy'] = 'sale'
        type_col = 'Event_Type_Dummy'

    def classify_qty(row):
        etype = str(row[type_col]).lower()
        qty = row[qty_col]
        
        sale_q, ret_q, can_q = 0, 0, 0
        
        if 'cancel' in etype:
            can_q = qty
        elif 'return' in etype:
            ret_q = qty 
        else:
            sale_q = qty
            
        return pd.Series([sale_q, ret_q, can_q])

    sales_df[['Sale_Qty', 'Return_Qty', 'Cancel_Qty']] = sales_df.apply(classify_qty, axis=1)

    # 4. Aggregation
    sku_stats = sales_df.groupby('SKU').agg({
        'Sale_Qty': 'sum',
        'Return_Qty': 'sum',
        'Cancel_Qty': 'sum',
        val_col: 'sum' 
    }).reset_index()

    sku_stats.rename(columns={val_col: 'Sum of Taxable Value'}, inplace=True)
    sku_stats['Net_Qty'] = sku_stats['Sale_Qty'] - sku_stats['Return_Qty']
    
    # 5. Process Cashback
    sku_map = sales_df[['Order Item ID', 'SKU']].drop_duplicates(subset='Order Item ID').set_index('Order Item ID')['SKU'].to_dict()
    cb_per_sku = pd.DataFrame(columns=['SKU', 'Cashback'])
    
    if cashback_df is not None and not cashback_df.empty:
        cashback_df = clean_col_names(cashback_df)
        if 'Order Item ID' in cashback_df.columns:
            cashback_df['Order Item ID'] = cashback_df['Order Item ID'].apply(clean_id)
            
            # Find cashback amount column
            cb_col = None
            for c in cashback_df.columns:
                 if 'taxable' in c.lower() and 'value' in c.lower():
                     cb_col = c
                     break
                     
            if cb_col:
                cashback_df[cb_col] = pd.to_numeric(cashback_df[cb_col], errors='coerce').fillna(0)
                cashback_df['SKU'] = cashback_df['Order Item ID'].map(sku_map).fillna('Unknown_SKU')
                cb_per_sku = cashback_df.groupby('SKU')[cb_col].sum().reset_index()
                cb_per_sku.rename(columns={cb_col: 'Cashback'}, inplace=True)

    # 6. Merge
    if cb_per_sku.empty:
        final_df = sku_stats.copy()
        final_df['Cashback'] = 0.0
    else:
        final_df = pd.merge(sku_stats, cb_per_sku, on='SKU', how='outer')
    
    cols_to_fill = ['Sale_Qty', 'Return_Qty', 'Cancel_Qty', 'Net_Qty', 'Sum of Taxable Value', 'Cashback']
    for c in cols_to_fill:
        if c in final_df.columns: final_df[c] = final_df[c].fillna(0)
        
    final_df['Total'] = final_df['Sum of Taxable Value'] + final_df['Cashback']
    
    # Apply Title mapping safely
    def get_title(sku):
        return title_map.get(sku, "Unknown Product")
        
    final_df['Product Name'] = final_df['SKU'].apply(get_title)
    
    return final_df

def generate_report_bytes(current_file, prev_file=None):
    """Generates the Excel report and returns it as a BytesIO object for download."""
    
    current_df = process_single_month_data(current_file, current_file.name)
    
    comparison_df = None
    if prev_file is not None:
        prev_df = process_single_month_data(prev_file, prev_file.name)
        
        # Ensure we have the needed columns before merging
        curr_cols = ['SKU', 'Product Name', 'Net_Qty', 'Total']
        prev_cols = ['SKU', 'Net_Qty', 'Total']
        
        # Check if current_df has the columns
        for c in curr_cols:
             if c not in current_df.columns:
                 current_df[c] = 0 if c != 'Product Name' else "Unknown"
                 
        # Check if prev_df has the columns
        for c in prev_cols:
             if c not in prev_df.columns:
                 prev_df[c] = 0
                 
        comparison_df = pd.merge(
            current_df[curr_cols],
            prev_df[prev_cols],
            on='SKU', 
            how='outer', 
            suffixes=('_Curr', '_Prev')
        )
        
        comparison_df = comparison_df.fillna({
            'Net_Qty_Curr': 0, 'Total_Curr': 0, 
            'Net_Qty_Prev': 0, 'Total_Prev': 0,
            'Product Name': 'Unknown Product'
        })
        
        comparison_df['Qty_Diff'] = comparison_df['Net_Qty_Curr'] - comparison_df['Net_Qty_Prev']
        comparison_df['Revenue_Diff'] = comparison_df['Total_Curr'] - comparison_df['Total_Prev']
        comparison_df.sort_values(by='Revenue_Diff', ascending=False, inplace=True)

    # --- Formatting Outputs ---
    cols_order = [
        'SKU', 'Product Name', 
        'Sale_Qty', 'Return_Qty', 'Cancel_Qty', 'Net_Qty', 
        'Sum of Taxable Value', 'Cashback', 'Total'
    ]
    
    # Ensure all columns exist before selecting
    for c in cols_order:
         if c not in current_df.columns:
             current_df[c] = 0
             
    detailed_sheet = current_df[cols_order].copy()
    detailed_sheet.sort_values(by='Total', ascending=False, inplace=True)

    summary_sheet = detailed_sheet[['SKU', 'Product Name', 'Net_Qty', 'Total']].copy()

    # --- Export to BytesIO ---
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        detailed_sheet.to_excel(writer, sheet_name='Detailed Breakdown', index=False)
        
        if comparison_df is not None and not comparison_df.empty:
            comparison_df.to_excel(writer, sheet_name='Prev Month Comparison', index=False)
            
        summary_sheet.to_excel(writer, sheet_name='Quick Summary', index=False)

        # Formatting
        workbook = writer.book
        money_fmt = workbook.add_format({'num_format': '#,##0.00'})
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})
        
        # Format Detailed Sheet
        ws_det = writer.sheets['Detailed Breakdown']
        ws_det.set_column('A:A', 20)
        ws_det.set_column('B:B', 40)
        ws_det.set_column('C:F', 12)
        ws_det.set_column('G:I', 20, money_fmt)
        for col_num, value in enumerate(detailed_sheet.columns.values):
             ws_det.write(0, col_num, value, header_fmt)
             
        # Format Comparison Sheet
        if comparison_df is not None and not comparison_df.empty:
            ws_comp = writer.sheets['Prev Month Comparison']
            ws_comp.set_column('A:A', 20)
            ws_comp.set_column('B:B', 40)
            ws_comp.set_column('C:H', 15)
            # Apply money format to Total and Revenue Diff columns
            for i, col in enumerate(comparison_df.columns):
                ws_comp.write(0, i, col, header_fmt)
                if 'Total' in col or 'Revenue' in col:
                     ws_comp.set_column(i, i, 18, money_fmt)

        # Format Summary Sheet
        ws_sum = writer.sheets['Quick Summary']
        ws_sum.set_column('A:A', 20)
        ws_sum.set_column('B:B', 40)
        ws_sum.set_column('C:C', 12)
        ws_sum.set_column('D:D', 20, money_fmt)
        for col_num, value in enumerate(summary_sheet.columns.values):
             ws_sum.write(0, col_num, value, header_fmt)

    return output.getvalue(), detailed_sheet

# ==========================================
# STREAMLIT UI
# ==========================================
def main():
    st.title("🛒 Flipkart Master Report Generator")
    st.markdown("Upload your Flipkart settlement reports to generate a cleaned, consolidated master Excel file.")

    with st.sidebar:
        st.header("1. Upload Data")
        
        curr_file = st.file_uploader(
            "Upload Current Month Report", 
            type=["xlsx", "xls"],
            help="The main Flipkart report containing 'Sales Report' and 'Cash Back Report' sheets."
        )
        
        prev_file = st.file_uploader(
            "Upload Previous Month Report (Optional)", 
            type=["xlsx", "xls"],
            help="Upload last month's report to generate a Comparison sheet."
        )
        
        st.divider()
        process_btn = st.button("🚀 Generate Report", use_container_width=True, type="primary")

    if process_btn:
        if not curr_file:
            st.error("Please upload the Current Month Report to proceed.")
            return

        with st.spinner("Processing data and generating report..."):
            try:
                # Generate Excel bytes
                excel_data, preview_df = generate_report_bytes(curr_file, prev_file)
                
                st.success("✅ Analysis Complete! Your report is ready.")
                
                # Show summary metrics
                col1, col2, col3 = st.columns(3)
                col1.metric("Total SKUs Processed", len(preview_df))
                col2.metric("Total Net Quantity", f"{preview_df['Net_Qty'].sum():,.0f}")
                col3.metric("Total Taxable Value", f"₹ {preview_df['Total'].sum():,.2f}")
                
                # Provide download button
                out_name = f"Flipkart_Master_Report_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx"
                
                st.download_button(
                    label="⬇️ Download Master Report (Excel)",
                    data=excel_data,
                    file_name=out_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )
                
                with st.expander("Preview Processed Data"):
                    st.dataframe(preview_df.head(50))
                    
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.exception(e) # Show full traceback for debugging

if __name__ == "__main__":
    main()
