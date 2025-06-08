def klasyfikuj_typ_przestepstwa(desc):
    desc = desc.lower()

    if any(word in desc for word in ["murder", "homicide", "manslaughter"]):
        return "Zabójstwa"

    elif any(word in desc for word in ["theft", "burglary", "shoplifting", "embezzlement", "larceny", "stealing", "robbery", "pickpocket", "purse snatching", "coin machine", "till tap", "vehicle", "stolen", "auto repair"]):
        return "Kradzież"

    elif any(word in desc for word in ["assault", "battery", "lynching", "resisting arrest", "kidnapping", "intimate partner", "false imprisonment"]):
        return "Przemoc fizyczna"

    elif any(word in desc for word in ["rape", "sexual", "lewd", "oral copulation", "sodomy", "indecent exposure", "pornography", "incest", "child abuse", "bestiality"]):
        return "Przemoc seksualna"

    elif any(word in desc for word in ["drug", "narcotic"]):
        return "Przestępstwa narkotykowe"

    elif any(word in desc for word in ["fraud", "bunco", "credit card", "forgery", "counterfeit", "defrauding", "dishonest employee", "bribery", "bigamy"]):
        return "Oszustwa i fałszerstwa"

    elif any(word in desc for word in ["firearm", "weapon", "shots", "brandish", "bomb", "explosive", "gun"]):
        return "Przestępstwa z użyciem broni"

    elif any(word in desc for word in ["trespassing", "disturbing", "disrupt", "prowler", "vandalism", "riot", "disorder", "school", "loitering"]):
        return "Zakłócanie porządku"

    else:
        return "Inne"
