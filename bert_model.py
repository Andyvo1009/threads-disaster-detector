import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from peft import PeftModel
from langchain_google_genai import ChatGoogleGenerativeAI
import os
from dotenv import load_dotenv
import threading

# Load environment variables
load_dotenv()

# Suppress warnings
import warnings
warnings.filterwarnings("ignore", message="Unexpected keyword arguments.*for class LoraConfig")
warnings.filterwarnings("ignore", category=UserWarning, module="peft.config")
import requests,json    
# Singleton instance for DisasterPredictor
_predictor = None
_predictor_lock = threading.Lock()  # Thread-safe lock for initialization

def extract_location_disaster(sentence):
    """Extract location and disaster info using local Ollama model"""
    try:
        # Ollama API endpoint (default local installation)
        ollama_url = "http://localhost:11434/api/generate"
        
        # Use your preferred model (replace with your model name)
        model_name = "gemma3:1b"  # or "mistral", "codellama", etc.
        
        prompt = f"""Extract the following information from the sentence below:

- Location: The place where the event occurred. Add more details if possible; if the context only gives the state or province, you should provide the country.
- Disaster: The type of natural disaster (e.g., flood, earthquake, landslide, drought, storm, traffic accidents, etc.)

If any information is missing, return "None".

Sentence: "{sentence}"

Respond ONLY with a plain JSON object like this (without code block or markdown):

{{
  "location": "...",
  "disaster": "..."
}}"""

        # Prepare request payload
        payload = {
            "model": model_name,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 1,
                "num_predict": 100
            }
        }
        
        # Make request to Ollama
        response = requests.post(
            ollama_url, 
            json=payload, 
            headers={"Content-Type": "application/json"},
            timeout=120
        )
        
        if response.status_code == 200:
            result = response.json()
            response_text = result.get("response", "").strip()
            
            # Try to parse the JSON response
            try:
                # Clean the response (remove any markdown formatting)
                if "```" in response_text:
                    response_text = response_text.split("```")[1]
                    if response_text.startswith("json"):
                        response_text = response_text[4:]
                
                parsed_json = json.loads(response_text)
                return json.dumps(parsed_json)
            except json.JSONDecodeError:
                print(f"❌ Failed to parse JSON from Ollama response: {response_text}")
                return json.dumps({"location": "None", "disaster": "None"})
        else:
            print(f"❌ Ollama API error: {response.status_code}")
            return json.dumps({"location": "None", "disaster": "None"})
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error connecting to Ollama: {e}")
        return json.dumps({"location": "None", "disaster": "None"})
    except Exception as e:
        print(f"❌ Unexpected error in extract_location_disaster: {e}")
        return json.dumps({"location": "None", "disaster": "None"})


def get_predictor(model_path="distilBERT"):
    """Get the singleton instance of DisasterPredictor."""
    global _predictor
    # Double-checked locking for thread safety
    if _predictor is None:
        with _predictor_lock:  # Ensure thread-safe initialization
            if _predictor is None:  # Double-check inside lock
                print("Initializing DisasterPredictor...")
                _predictor = DisasterPredictor(model_path)
                print("DisasterPredictor initialized successfully.")
    return _predictor

def predict_disaster_udf(text):
    """UDF wrapper for disaster prediction"""
    if text is None or text.strip() == "":
        return "0,0.0"  # Return as string to be parsed later
    
    pred = DisasterPredictor()
    prediction, confidence = pred.predict_disaster(text)
    return f"{prediction},{confidence:.4f}"

class DisasterPredictor:
    """Class to handle DistilBERT model loading and prediction"""
    
    def __init__(self, model_path="distilBERT"):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
        # Load tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        
        # Load base model
        base_model = AutoModelForSequenceClassification.from_pretrained(
            "distilbert/distilbert-base-uncased",
            num_labels=2  # Binary classification for disaster/non-disaster
        )
        
        # Load PEFT adapter
        self.model = PeftModel.from_pretrained(base_model, model_path)
        self.model.to(self.device)
        self.model.eval()
        
        print(f"Model device: {next(self.model.parameters()).device}")

    def predict_disaster(self, text):
        """Predict if text is about a disaster using the trained model"""
        try:
            if not text or text.strip() == "":
                return 0, 0.0
                
            # Tokenize input
            inputs = self.tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                padding=True,
                max_length=512
            )
            
            # Move to device
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            # Get prediction
            with torch.no_grad():
                outputs = self.model(**inputs)
                predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
                predicted_class = torch.argmax(predictions, dim=-1).item()
                confidence = predictions[0][predicted_class].item()
            
            return int(predicted_class), float(confidence)
            
        except Exception as e:
            print(f"Error predicting for text: '{text[:50] if text else 'None'}...' Error: {str(e)}")
            return 0, 0.0  # Default to non-disaster with 0 confidence