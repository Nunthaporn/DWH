#!/usr/bin/env python3
"""
Test script สำหรับตรวจสอบประสิทธิภาพของ fact_sales_quotation job
"""

import time
import pandas as pd
import numpy as np
from jobs.fact_sales_quotation import extract_sales_quotation_data, clean_sales_quotation_data, load_sales_quotation_data

def test_extract_performance():
    """ทดสอบประสิทธิภาพของ extract function"""
    print("🧪 ทดสอบ Extract Performance...")
    start_time = time.time()
    
    try:
        df_plan, df_order, df_pay = extract_sales_quotation_data()
        extract_time = time.time() - start_time
        
        print(f"✅ Extract สำเร็จใน {extract_time:.2f} วินาที")
        print(f"📊 df_plan: {df_plan.shape}")
        print(f"📊 df_order: {df_order.shape}")
        print(f"📊 df_pay: {df_pay.shape}")
        
        return df_plan, df_order, df_pay, extract_time
    except Exception as e:
        print(f"❌ Extract ล้มเหลว: {e}")
        return None, None, None, None

def test_clean_performance(df_plan, df_order, df_pay):
    """ทดสอบประสิทธิภาพของ clean function"""
    print("\n🧪 ทดสอบ Clean Performance...")
    start_time = time.time()
    
    try:
        df_clean = clean_sales_quotation_data((df_plan, df_order, df_pay))
        clean_time = time.time() - start_time
        
        print(f"✅ Clean สำเร็จใน {clean_time:.2f} วินาที")
        print(f"📊 df_clean: {df_clean.shape}")
        print(f"📊 Memory usage: {df_clean.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        return df_clean, clean_time
    except Exception as e:
        print(f"❌ Clean ล้มเหลว: {e}")
        return None, None

def test_load_performance(df_clean, test_mode=True):
    """ทดสอบประสิทธิภาพของ load function"""
    print("\n🧪 ทดสอบ Load Performance...")
    start_time = time.time()
    
    try:
        if test_mode:
            # ทดสอบกับข้อมูลตัวอย่างเล็กๆ
            df_sample = df_clean.head(1000).copy()
            print(f"🧪 ทดสอบกับข้อมูลตัวอย่าง {len(df_sample)} rows")
        else:
            df_sample = df_clean
        
        load_sales_quotation_data(df_sample)
        load_time = time.time() - start_time
        
        print(f"✅ Load สำเร็จใน {load_time:.2f} วินาที")
        return load_time
    except Exception as e:
        print(f"❌ Load ล้มเหลว: {e}")
        return None

def main():
    """ฟังก์ชันหลักสำหรับทดสอบ"""
    print("🚀 เริ่มทดสอบประสิทธิภาพ fact_sales_quotation job...")
    
    # ทดสอบ Extract
    df_plan, df_order, df_pay, extract_time = test_extract_performance()
    if df_plan is None:
        return
    
    # ทดสอบ Clean
    df_clean, clean_time = test_clean_performance(df_plan, df_order, df_pay)
    if df_clean is None:
        return
    
    # ทดสอบ Load (แบบ test mode)
    load_time = test_load_performance(df_clean, test_mode=True)
    if load_time is None:
        return
    
    # สรุปผลการทดสอบ
    total_time = extract_time + clean_time + load_time
    print(f"\n📊 สรุปผลการทดสอบ:")
    print(f"   Extract: {extract_time:.2f} วินาที")
    print(f"   Clean: {clean_time:.2f} วินาที")
    print(f"   Load: {load_time:.2f} วินาที")
    print(f"   รวม: {total_time:.2f} วินาที")
    
    # ประมาณการเวลาสำหรับข้อมูลทั้งหมด
    if len(df_clean) > 0:
        estimated_full_load_time = (load_time / 1000) * len(df_clean)
        print(f"   ประมาณการเวลาสำหรับข้อมูลทั้งหมด: {estimated_full_load_time:.2f} วินาที ({estimated_full_load_time/60:.2f} นาที)")
    
    print("\n✅ การทดสอบเสร็จสิ้น!")

if __name__ == "__main__":
    main() 