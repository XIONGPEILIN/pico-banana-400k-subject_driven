#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
import sys
import os
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置 vllm 本地服务地址
# 注意：端口设置为 8001 以匹配 start_vllm.sh 的配置
VLLM_API_URL = "http://localhost:25874/v1"
VLLM_API_KEY = "EMPTY" 
MODEL_NAME = "Qwen/Qwen3-4B-Instruct-2507"

# 初始化客户端
# OpenAI 客户端是线程安全的，可以在多线程中共享
client = OpenAI(
    base_url=VLLM_API_URL,
    api_key=VLLM_API_KEY,
)

def extract_object_with_llm(description):
    """
    使用 LLM 从描述中提取主要物体（名词）。
    """
    if not description:
        return None

    prompt = (
        "You are a language understanding model.\n"
        "Task: Read the text below and identify the main physical object described "
        "(i.e., a tangible entity that can be seen or touched).\n"
        "Then output only the noun for that object (no adjectives or extra explanation).\n"
        'If the object cannot be determined clearly, output "unable to determine".\n\n'
        f'Text: "{description}"\n'
        "Object:"
    )

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "user", "content": prompt}
            ],
        )
        
        extracted_content = response.choices[0].message.content.strip()
        extracted_content = extracted_content.replace(".", "").lower()
        return extracted_content

    except Exception as e:
        # 在多线程中打印错误可能会乱序，但仍然有助于调试
        print(f"\nError calling vllm for description '{description}': {e}")
        return None

def process_single_item(item):
    """
    线程工作函数：处理单个 item。
    直接修改字典对象（Python 字典是可变的，线程间共享内存）。
    """
    description = item.get('description', '')
    extracted_word = []
    
    if description:
        noun = extract_object_with_llm(description)
        if noun:
            extracted_word = [noun]
    
    # 原地修改 item
    item['extracted_word'] = extracted_word
    return bool(extracted_word)

if __name__ == '__main__':
    bbox_results_path = 'bbox_results.json'
    
    # 1. Load Data
    if not os.path.exists(bbox_results_path):
        # 创建更多数据用于测试多线程效果
        print(f"Creating dummy {bbox_results_path} for testing...")
        dummy_data = [{
            "image_id": 1001,
            "meta_info": "This should be preserved",
            "bbox": {
                "objects": [{"description": f"Test object {i}"} for i in range(100)]
            }
        }]
        with open(bbox_results_path, 'w') as f:
            json.dump(dummy_data, f)

    try:
        with open(bbox_results_path, 'r', encoding='utf-8') as f:
            external_data = json.load(f)
    except Exception as e:
        print(f"Error loading JSON: {e}")
        exit()
    
    # 统一处理格式：无论是 list 还是 dict，都放入 data_entries 列表进行遍历
    # 注意：data_entries 中的元素是 external_data 中对象的引用，修改它会直接修改 external_data
    data_entries = external_data if isinstance(external_data, list) else [external_data]
    
    # 2. 收集所有需要处理的任务 (Flatten tasks)
    # 我们先遍历一遍结构，把需要处理的 item 对象收集起来
    all_items_to_process = []
    
    print("Preparing tasks...")
    for entry in data_entries:
        if 'bbox' in entry and 'objects' in entry['bbox']:
            for item in entry['bbox']['objects']:
                all_items_to_process.append(item)
    
    # --- TEST MODE: 限制只处理前 1000 个对象 ---
    TEST_LIMIT = 1000
    if len(all_items_to_process) > TEST_LIMIT:
        print(f"\n*** TEST MODE ACTIVATED ***")
        print(f"Limiting tasks from {len(all_items_to_process)} to first {TEST_LIMIT} objects.")
        all_items_to_process = all_items_to_process[:TEST_LIMIT]
    # ----------------------------------------

    total_tasks = len(all_items_to_process)
    print(f"Total objects to process: {total_tasks}")
    
    start_time = time.time()
    
    # 3. 使用 ThreadPoolExecutor 进行 64 线程并发处理
    # max_workers=64 意味着同时会有 64 个请求发往 vllm
    print(f"Starting processing using model: {MODEL_NAME} with 64 threads...")
    
    success_count = 0
    completed_count = 0

    with ThreadPoolExecutor(max_workers=64) as executor:
        # 提交所有任务
        futures = {executor.submit(process_single_item, item): item for item in all_items_to_process}
        
        # 处理结果 (as_completed 会在任务完成时立即 yield)
        for future in as_completed(futures):
            completed_count += 1
            try:
                if future.result():
                    success_count += 1
            except Exception as e:
                print(f"Thread exception: {e}")
            
            # 简单的进度显示
            if completed_count % 10 == 0 or completed_count == total_tasks:
                sys.stdout.write(f"\rProgress: {completed_count}/{total_tasks} ({(completed_count/total_tasks)*100:.1f}%)")
                sys.stdout.flush()

    end_time = time.time()
    
    print(f"\n\n--- Processing Complete in {end_time - start_time:.2f} seconds ---")
    print(f"Success rate: {success_count}/{total_tasks}")
    
    # 4. Save Results (Saving the FULL external_data)
    # 这里的 external_data 已经被原地修改了，包含了提取的信息，同时也保留了所有原始字段
    output_filename = "bbox_results_processed.json"
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(external_data, f, indent=4, ensure_ascii=False)
    
    print(f"Full results saved to {output_filename}")
    
    # 仅打印前几个作为示例
    preview_data = external_data[0] if isinstance(external_data, list) and external_data else external_data
    print("Preview of first entry (structure preserved):")
    print(json.dumps(preview_data, indent=4, ensure_ascii=False))