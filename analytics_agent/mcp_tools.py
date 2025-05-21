from typing import Dict, Any, List
import re
import matplotlib.pyplot as plt
import base64
import io

def extract_parameters(prompt: str) -> Dict[str, Any]:
    params = {}
    # Skill extraction
    skills = re.findall(r'\b(python|java|sql|aws|react)\b', prompt, re.IGNORECASE)
    if skills:
        params['skills'] = list(set([s.lower() for s in skills]))
    
    # Experience extraction
    exp_match = re.search(r'(\d+)\+? years? experience', prompt)
    if exp_match:
        params['min_experience'] = int(exp_match.group(1))
        
    # Location extraction
    locations = re.findall(r'\b(london|new york|remote)\b', prompt, re.IGNORECASE)
    if locations:
        params['locations'] = list(set([loc.title() for loc in locations]))
        
    return params

def generate_visualization(query_results: Dict[str, Any]):
    # Simple visualization for POC
    if "results" in query_results:
        data = query_results["results"]
        
        # Create bar chart
        fig, ax = plt.subplots()
        ax.bar([str(x) for x in range(len(data))], [len(d) for d in data])
        
        # Convert to base64
        buf = io.BytesIO()
        fig.savefig(buf, format='png')
        return {"image": base64.b64encode(buf.getvalue()).decode('utf-8')}
    
    return {"error": "No data to visualize"}

def natural_language_to_sql(prompt: str) -> Dict[str, Any]:
    # Simple conversion for POC
    return {
        "query": f"SELECT * FROM candidates WHERE {extract_conditions(prompt)}",
        "parameters": extract_parameters(prompt)
    }

def extract_conditions(prompt: str) -> str:
    conditions = []
    if "python" in prompt.lower():
        conditions.append("skills LIKE '%Python%'")
    if "experience" in prompt.lower():
        conditions.append("experience_years >= 5")
    return " AND ".join(conditions) if conditions else "1=1"