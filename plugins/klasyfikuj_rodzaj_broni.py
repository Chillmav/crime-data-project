import pandas as pd

def klasyfikuj_rodzaj_broni(bron):

    if not isinstance(bron, str):
        return "Brak"  # lub "Nieznana", zależnie od Twojej klasyfikacji

    bron = bron.lower()

    if "strong-arm" in bron or "bodily force" in bron:
        return "Brak"
    elif any(w in bron for w in ["gun", "firearm", "pistol", "revolver", "rifle", "weapon", "shotgun", "uzi", "hk", "mac", "starter", "air"]):
        return "Palna"
    elif any(w in bron for w in ["knife", "razor", "dagger", "sword", "machete", "axe", "scissors", "cutting", "sharp", "ice pick", "screwdriver", "cleaver"]):
        return "Biała"
    elif any(w in bron for w in ["verbal", "demand note", "threat"]):
        return "Werbalna"
    else:
        return "Nieznana"
