# test_chat_simple.py
import requests
import json
import time

def test_chat():
    print("Testing chat with deepseek-r1...")
    
    payload = {
        "model": "deepseek-r1:latest",
        "messages": [{"role": "user", "content": "Hello, respond with just 'OK'"}],
        "stream": False,
        "options": {"temperature": 0.1}
    }
    
    try:
        start_time = time.time()
        response = requests.post("http://localhost:11434/api/chat", json=payload, timeout=60)
        end_time = time.time()
        
        print(f"Response time: {end_time - start_time:.2f} seconds")
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Chat successful!")
            print(f"Response: {json.dumps(result, indent=2)}")
            
            if 'message' in result and 'content' in result['message']:
                print(f"Content: {result['message']['content']}")
            else:
                print("Unexpected response format")
        else:
            print(f"❌ Error: {response.status_code} - {response.text}")
            
    except requests.exceptions.Timeout:
        print("❌ Request timed out. Model might be still loading.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_chat()