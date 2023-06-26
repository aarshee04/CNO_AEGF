import pandas as pd

def concat(value):
    global df
    #return value + " " + df['last'][index]
    #print(df['last'][index])
    print("--------")
    print(value.name)
    return ""

data = {"first": ["Raveendra", "Smitha", "Aarav"], "middle": ["G", "K", "S"], "last": ["Nanj", "Putta", "Ravi"] }

df = pd.DataFrame(data)

# construct the string in this format: "{0[first]}-{0[middle]}#{0[last]}"
format_str = "{0[" + df.columns[0] + "]}" + "-" + "{0[" + df.columns[1] + "]}" + "#" + "{0[" + df.columns[2] + "]}"
#print(df.agg(format_str.format, axis=1))

#newdf = df["first"].apply(concat)
#newdf = df.apply(lambda row: concat(row), axis = 0)
#print(newdf)

#print(df.agg(format_str.format, axis=1))
