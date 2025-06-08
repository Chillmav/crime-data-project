import pandas as pd

def klasyfikuj_status(desc):

    dict = {

        "Juv Arrest": "Nieletni aresztowany",
        "Juv Other": "Nieletni nie aresztowany",
        "UNK": "Nieznany",
        "Invest Cont": "Śledztwo trwa",
        "Adult Other": "Dorosły nie aresztowany",
        "Adult Arrest": "Dorosły aresztowany"
        
    }

    return dict.get(desc, "Nieznany")