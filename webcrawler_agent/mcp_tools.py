import random

# Sample data for random name generation and extra skills
FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Ethan", "Fatima", "George", "Hana"]
LAST_NAMES = ["Smith", "Johnson", "Lee", "Patel", "Garcia", "Mori", "Chen", "Brown"]
EXTRA_SKILLS = ["Communication", "Team Leadership", "Project Management", "Problem Solving"]

def list_candidates(title: str, skills: str):
    """
    Generate 5 mock candidate profiles that match the given job title and required skills.
    """
    candidates = []
    # Normalize skills input (e.g., comma-separated string to list)
    if isinstance(skills, str):
        base_skills = [s.strip() for s in skills.split(',') if s.strip()]
    else:
        base_skills = list(skills)  # if already a list
    for i in range(5):
        full_name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        # Combine given skills with one random extra skill to simulate a profile
        candidate_skills = base_skills.copy()
        extra_skill = random.choice(EXTRA_SKILLS)
        if extra_skill not in candidate_skills:
            candidate_skills.append(extra_skill)
        experience_years = random.randint(1, 20)
        candidates.append({
            "id": f"cand_{i+1}",  # simple unique ID for mock
            "name": full_name,
            "title": title,
            "skills": candidate_skills,
            "experience": f"{experience_years} years"
        })
    return candidates
