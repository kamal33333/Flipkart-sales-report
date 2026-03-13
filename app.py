import pandas as pd
import os
import re

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

def load_sheet_robust(file_path, sheet_name):
    """Loads a sheet by searching for the header row dynamically."""
    try:
        xl = pd.ExcelFile(file_path)
        if sheet_name not in xl.sheet_names:
            return None
        
        # Scan first 20 rows to find header
        preview = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=20)
        header_idx = -1
        for idx, row in preview.iterrows():
            if row.astype(str).str.contains('Order Item ID', case=False, na=False).any():
                header_idx = idx
                break
        
        if header_idx == -1: return None
        return pd.read_excel(file_path, sheet_name=sheet_name, header=header_idx)
    except Exception as e:
        print(f"Error loading {sheet_name} from {os.path.basename(file_path)}: {e}")
        return None

def process_single_month_data(file_path):
    """
    Process a single month's file.
    """
    print(f"Processing: {os.path.basename(file_path)}...")
    
    # 1. Load Data
    sales_df = load_sheet_robust(file_path, 'Sales Report')
    cashback_df = load_sheet_robust(file_path, 'Cash Back Report')
    
    if sales_df is None:
        raise ValueError("Sales Report sheet missing.")

    # 2. Clean Sales Data
    sales_df = clean_col_names(sales_df)
    sales_df['Order Item ID'] = sales_df['Order Item ID'].apply(clean_id)
    
    # SKU Cleaning
    if 'SKU' in sales_df.columns:
        sales_df['SKU'] = sales_df['SKU'].apply(clean_sku)
    else:
        sales_df['SKU'] = 'Unknown_SKU'

    # Map SKU to Title
    if 'Product Title/Description' in sales_df.columns:
        sales_df['Product Title/Description'] = sales_df['Product Title/Description'].apply(lambda x: str(x).replace('"', '').strip())
        title_map = sales_df.groupby('SKU')['Product Title/Description'].first().to_dict()
    else:
        title_map = {}

    # Ensure Numeric Columns
    qty_col = 'Item Quantity'
    val_col = 'Taxable Value (Final Invoice Amount -Taxes)' 
    
    # Robust column finding
    if val_col not in sales_df.columns:
        possible_cols = [c for c in sales_df.columns if 'Taxable Value' in c]
        if possible_cols:
            val_col = possible_cols[0]
        else:
            print("CRITICAL WARNING: Could not find 'Taxable Value' column. Using 0.")
            sales_df['Taxable Value_Dummy'] = 0
            val_col = 'Taxable Value_Dummy'

    sales_df[qty_col] = pd.to_numeric(sales_df[qty_col], errors='coerce').fillna(0)
    sales_df[val_col] = pd.to_numeric(sales_df[val_col], errors='coerce').fillna(0)

    # 3. Categorize Transactions (For QUANTITY counts only)
    type_col = 'Event Sub Type'
    if type_col not in sales_df.columns: type_col = 'Event Type'

    def classify_qty(row):
        etype = str(row[type_col]).lower()
        qty = row[qty_col]
        
        sale_q = 0
        ret_q = 0
        can_q = 0
        
        if 'cancel' in etype:
            can_q = qty
        elif 'return' in etype:
            ret_q = qty 
        else:
            sale_q = qty
            
        return pd.Series([sale_q, ret_q, can_q])

    sales_df[['Sale_Qty', 'Return_Qty', 'Cancel_Qty']] = sales_df.apply(classify_qty, axis=1)

    # 4. Aggregation
    # IMPORTANT: We use simple 'sum' for val_col to capture the raw negative values of returns/cancellations correctly.
    sku_stats = sales_df.groupby('SKU').agg({
        'Sale_Qty': 'sum',
        'Return_Qty': 'sum',
        'Cancel_Qty': 'sum',
        val_col: 'sum' # The raw sum (User confirmed this is the correct source of truth)
    }).reset_index()

    sku_stats.rename(columns={val_col: 'Sum of Taxable Value'}, inplace=True)
    sku_stats['Net_Qty'] = sku_stats['Sale_Qty'] - sku_stats['Return_Qty']
    
    # 5. Process Cashback
    sku_map = sales_df[['Order Item ID', 'SKU']].drop_duplicates(subset='Order Item ID').set_index('Order Item ID')['SKU'].to_dict()
    cb_per_sku = pd.DataFrame(columns=['SKU', 'Cashback'])
    
    if cashback_df is not None:
        cashback_df = clean_col_names(cashback_df)
        cashback_df['Order Item ID'] = cashback_df['Order Item ID'].apply(clean_id)
        
        cb_col = next((c for c in cashback_df.columns if 'taxable' in c.lower() and 'value' in c.lower()), None)
        
        if cb_col:
            cashback_df[cb_col] = pd.to_numeric(cashback_df[cb_col], errors='coerce').fillna(0)
            cashback_df['SKU'] = cashback_df['Order Item ID'].map(sku_map).fillna('Unknown_SKU')
            
            cb_per_sku = cashback_df.groupby('SKU')[cb_col].sum().reset_index()
            cb_per_sku.rename(columns={cb_col: 'Cashback'}, inplace=True)

    # 6. Merge
    final_df = pd.merge(sku_stats, cb_per_sku, on='SKU', how='outer')
    
    # Fill NaNs
    cols_to_fill = ['Sale_Qty', 'Return_Qty', 'Cancel_Qty', 'Net_Qty', 'Sum of Taxable Value', 'Cashback']
    for c in cols_to_fill:
        if c in final_df.columns: final_df[c] = final_df[c].fillna(0)
        
    final_df['Total'] = final_df['Sum of Taxable Value'] + final_df['Cashback']
    final_df['Product Name'] = final_df['SKU'].map(title_map).fillna("Unknown Product")
    
    return final_df

def generate_full_report(current_file, prev_file, output_folder):
    # --- Process Current ---
    current_df = process_single_month_data(current_file)
    
    # --- Process Previous (Optional) ---
    comparison_df = None
    if prev_file and os.path.exists(prev_file):
        prev_df = process_single_month_data(prev_file)
        
        comparison_df = pd.merge(
            current_df[['SKU', 'Product Name', 'Net_Qty', 'Total']],
            prev_df[['SKU', 'Net_Qty', 'Total']],
            on='SKU', 
            how='outer', 
            suffixes=('_Curr', '_Prev')
        )
        
        comparison_df = comparison_df.fillna(0)
        comparison_df['Qty_Diff'] = comparison_df['Net_Qty_Curr'] - comparison_df['Net_Qty_Prev']
        comparison_df['Revenue_Diff'] = comparison_df['Total_Curr'] - comparison_df['Total_Prev']
        comparison_df.sort_values(by='Revenue_Diff', ascending=False, inplace=True)

    # --- Formatting Outputs ---
    
    # Sheet 1: Detailed Breakdown
    cols_order = [
        'SKU', 'Product Name', 
        'Sale_Qty', 'Return_Qty', 'Cancel_Qty', 'Net_Qty', 
        'Sum of Taxable Value', 'Cashback', 'Total'
    ]
    
    detailed_sheet = current_df[cols_order]
    detailed_sheet.sort_values(by='Total', ascending=False, inplace=True)

    # Sheet 2: Quick Summary
    summary_sheet = detailed_sheet[['SKU', 'Product Name', 'Net_Qty', 'Total']]

    # --- Export ---
    if output_folder:
        if not os.path.exists(output_folder): os.makedirs(output_folder)
        base_dir = output_folder
    else:
        base_dir = os.path.dirname(current_file)
        
    out_name = f"Flipkart_Master_Report_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx"
    out_path = os.path.join(base_dir, out_name)

    try:
        with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
            detailed_sheet.to_excel(writer, sheet_name='Detailed Breakdown', index=False)
            if comparison_df is not None:
                comparison_df.to_excel(writer, sheet_name='Prev Month Comparison', index=False)
            summary_sheet.to_excel(writer, sheet_name='Quick Summary', index=False)

            # Format money columns
            workbook = writer.book
            money_fmt = workbook.add_format({'num_format': '#,##0.00'})
            
            ws_det = writer.sheets['Detailed Breakdown']
            ws_det.set_column('B:B', 40)
            ws_det.set_column('G:I', 15, money_fmt)

        print(f"\nSUCCESS! Report generated at:\n{out_path}")
        
    except Exception as e:
        print(f"Error saving file: {e}")

if __name__ == "__main__":
    # --- INPUTS ---
    curr_file = r"C:\Users\Kamalkishore\Downloads\4a4586ca-2580-455f-82a7-50d3ea83616e_1772701789000.xlsx"
    # prev_file = r"C:\Users\Kamalkishore\Downloads\Previous_Month.xlsx" 
    prev_file = None # Set to None if no previous file
    
    out_folder = r"D:\Powerbi dash ecom\Flipkart\Summaries"
    
    generate_full_report(curr_file, prev_file, out_folder)
