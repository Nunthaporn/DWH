#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from jobs.fact_installment_payments import extract_installment_data, clean_installment_data
import pandas as pd

def debug_payment_amount():
    print("🔍 Starting debug for payment_amount...")
    
    # 1. Extract data
    print("\n📦 Step 1: Extracting data...")
    df_raw = extract_installment_data()
    print("✅ Extract completed")
    
    # 2. Clean data
    print("\n🧹 Step 2: Cleaning data...")
    df_clean = clean_installment_data(df_raw)
    print("✅ Clean completed")
    
    # 3. Analyze payment_amount
    print("\n📊 Step 3: Analyzing payment_amount...")
    print(f"📊 Final DataFrame shape: {df_clean.shape}")
    print(f"📊 payment_amount non-null: {df_clean['payment_amount'].notna().sum()}")
    print(f"📊 payment_amount null: {df_clean['payment_amount'].isna().sum()}")
    print(f"📊 payment_amount dtype: {df_clean['payment_amount'].dtype}")
    
    if df_clean['payment_amount'].notna().sum() > 0:
        print(f"📊 payment_amount sample values: {df_clean['payment_amount'].dropna().head(10).tolist()}")
        print(f"📊 payment_amount unique values: {df_clean['payment_amount'].dropna().nunique()}")
        print(f"📊 payment_amount min: {df_clean['payment_amount'].min()}")
        print(f"📊 payment_amount max: {df_clean['payment_amount'].max()}")
    else:
        print("❌ No non-null values in payment_amount!")
    
    # 4. Check other columns for comparison
    print("\n📊 Step 4: Comparing with other columns...")
    for col in ['installment_amount', 'total_paid', 'late_fee']:
        if col in df_clean.columns:
            print(f"📊 {col}: {df_clean[col].notna().sum()} non-null, {df_clean[col].isna().sum()} null")
    
    return df_clean

if __name__ == "__main__":
    df_result = debug_payment_amount()
    print("\n�� Debug completed!") 