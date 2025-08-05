#!/usr/bin/env python3
"""
Test script สำหรับตรวจสอบประสิทธิภาพของ fact_sales_quotation job
"""

import time
import pandas as pd
from jobs.dim_card_agent import extract_card_agent_data, clean_card_agent_data, load_card_agent_data

def test_performance():
    print("🚀 เริ่มทดสอบประสิทธิภาพ...")
    
    # ทดสอบการ extract
    start_time = time.time()
    print("📦 กำลัง extract ข้อมูล...")
    df_raw = extract_card_agent_data()
    extract_time = time.time() - start_time
    print(f"✅ Extract เสร็จสิ้นใน {extract_time:.2f} วินาที")
    
    # ทดสอบการ clean
    start_time = time.time()
    print("🧹 กำลัง clean ข้อมูล...")
    df_clean = clean_card_agent_data(df_raw)
    clean_time = time.time() - start_time
    print(f"✅ Clean เสร็จสิ้นใน {clean_time:.2f} วินาที")
    
    # ทดสอบการ load
    start_time = time.time()
    print("💾 กำลัง load ข้อมูล...")
    load_card_agent_data(df_clean)
    load_time = time.time() - start_time
    print(f"✅ Load เสร็จสิ้นใน {load_time:.2f} วินาที")
    
    total_time = extract_time + clean_time + load_time
    print(f"\n📊 สรุปประสิทธิภาพ:")
    print(f"   Extract: {extract_time:.2f} วินาที")
    print(f"   Clean: {clean_time:.2f} วินาที")
    print(f"   Load: {load_time:.2f} วินาที")
    print(f"   รวมทั้งหมด: {total_time:.2f} วินาที ({total_time/60:.2f} นาที)")
    
    if total_time < 300:  # น้อยกว่า 5 นาที
        print("🎉 ปรับปรุงสำเร็จ! โค้ดใหม่เร็วกว่าเดิมมาก")
    elif total_time < 600:  # น้อยกว่า 10 นาที
        print("👍 ปรับปรุงสำเร็จ! โค้ดใหม่เร็วกว่าเดิม")
    else:
        print("⚠️ ยังต้องปรับปรุงเพิ่มเติม")

if __name__ == "__main__":
    test_performance() 