column = {
    "identifier": "lob",
    "hidden": False,
    "justification": 0,
    "caption": "LOB",
    "data_source": [
        "$LOB$"
    ],
    "transformations": [
        {
            "function": "lob_lookup2"
        },
        {
            "function": "ssn_lookup"
        }
    ]
}

# using any
print("transformations" in column and any(trans["function"] in ["lob_lookup", "deduction_lookup"] for trans in column["transformations"] if "function" in trans))

# using filter
#lob_lookup = next(filter(lambda trans: trans["function"] == "lob_lookup", column["transformations"]), {})
#print(lob_lookup)
