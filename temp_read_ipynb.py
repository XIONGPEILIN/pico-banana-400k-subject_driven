import json

try:
    with open('tools/sam3/examples/sam3_agent.ipynb', 'r', encoding='utf-8') as f:
        data = json.load(f)
        print(json.dumps(data, indent=2))
except Exception as e:
    print(f"Error reading or parsing notebook: {e}")
