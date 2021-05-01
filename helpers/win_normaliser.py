normalised_results = {
    "Decision - Unanimous": "UD",
    "KO/TKO": "KO",
    "Submission": "SUB",
    "Decision - Split": "SD",
    "TKO - Doctor's Stoppage": "KO",
    "Decision - Majority": "UD",
    "Overturned": "NA",
    "DQ": "NA",
    "Could Not Continue": "NA",
    "Other": "NA"
}

win_result_weight = {
    "main": {
        "UD": 1.0,
        "KO": 1.0,
        "SD": 0.75,
        "SUB": 1.0
    },
    "other": {
        "UD": 0.8,
        "KO": 1.0,
        "SD": 0.6,
        "SUB": 1.0
    }
}

