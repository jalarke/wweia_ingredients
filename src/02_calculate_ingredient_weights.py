# Import packages
import pandas as pd

# -*- coding: utf-8 -*-
"""
Created on Sat Apr  1 20:21:57 2023

@author: Trevor Chan
"""

FILENAME = '../data/01/fndds_16_all_ingredients.csv'

def preprocessing(df):
    df = df[['parent_foodcode', 'parent_desc', 'ingred_code1', 'ingred_desc1', 'ingred_code2', 'ingred_desc2', 'ingred_wt2', 'ingred_wt1']].copy()
    
    
    df_1 = df[['parent_foodcode', 'parent_desc', 'ingred_code1', 'ingred_desc1', 'ingred_wt1']].copy()
    df_2 = df[['ingred_code1', 'ingred_desc1', 'ingred_code2', 'ingred_desc2', 'ingred_wt2']].copy().rename(columns={'ingred_code1': 'parent_foodcode', 
                                                                                                     'ingred_desc1': 'parent_desc',
                                                                                                     'ingred_code2': 'ingred_code1',
                                                                                                     'ingred_desc2': 'ingred_desc1',
                                                                                                     'ingred_wt2': 'ingred_wt1'}, inplace=False)
    df_parent = pd.concat([df_1, df_2]).drop_duplicates()
    return df_parent

def get_ingredient_proportions(df, parent_foodcode, parent_multiplier=1.0):
    ingred_rows = df[df['parent_foodcode'] == parent_foodcode]
    final_ingredients = {}

    for _, row in ingred_rows.iterrows():
        ingred_code1 = row['ingred_code1']
        ingred_desc1 = row['ingred_desc1']
        ingred_wt1 = row['ingred_wt1']

        if len(str(int(ingred_code1))) < 8:
            if parent_multiplier == 1.0:
                scaled_proportion = ingred_wt1
            else:
                scaled_proportion = parent_multiplier * (ingred_wt1 / ingred_rows['ingred_wt1'].sum())
            final_ingredients[ingred_code1] = {"ingred_desc": ingred_desc1, "ingred_wt": scaled_proportion}
        else:
            sub_ingredients = get_ingredient_proportions(df, ingred_code1, ingred_wt1)
            for ingred_code, data in sub_ingredients.items():
                if ingred_code not in final_ingredients:
                    final_ingredients[ingred_code] = {"ingred_desc": data["ingred_desc"], "ingred_wt": 0}
                final_ingredients[ingred_code]["ingred_wt"] += data["ingred_wt"]

    return final_ingredients

def main():   
    df = pd.read_csv(FILENAME)  # Replace with the appropriate file path
    df = preprocessing(df) 
    
    all_foodcodes = df[df.columns[0]].unique()
    foodcode_ingredients = {}
    
    for foodcode in all_foodcodes:
        parent_desc = df.loc[df['parent_foodcode'] == foodcode, 'parent_desc'].iloc[0]
        ingredients_proportions = get_ingredient_proportions(df, foodcode)
        foodcode_ingredients[foodcode] = {"parent_desc": parent_desc, "ingredients": ingredients_proportions}
    
    aggregated_data = []
    for foodcode, data in foodcode_ingredients.items():
        for ingred_code, ingred_data in data['ingredients'].items():
            aggregated_data.append({
                'parent_foodcode': foodcode,
                'parent_desc': data['parent_desc'],
                'ingred_code': ingred_code,
                'ingred_desc': ingred_data['ingred_desc'],
                'ingred_wt': ingred_data['ingred_wt']
            })
    
    # Convert the list of dictionaries to a dataframe
    aggregated_16 = pd.DataFrame(aggregated_data)
    aggregated_16.to_csv('../data/02/fndds_16_ingredient_wt_corrected.csv', sep=',', encoding='utf-8', index=False)


main()

aggregated_16 = pd.read_csv('../data/02/fndds_16_ingredient_wt_corrected.csv')

fndds16 = pd.read_csv('../data/01/fndds_16_ingredient_codes.csv', usecols=['Food code', 'Main food description', 'Ingredient code', 'Ingredient description', 'Ingredient weight'])
fndds16.rename(columns={'Food code': 'parent_foodcode', 'Main food description': 'parent_desc', 'Ingredient code': 'ingred_code', 'Ingredient description': 'ingred_desc', 'Ingredient weight': 'ingred_wt'}, inplace=True)
fndds16 = fndds16[fndds16['ingred_code']<10000000]
fndds16_combined = pd.concat([aggregated_16, fndds16], ignore_index=True).drop_duplicates(keep='first')
fndds16_combined = fndds16_combined.groupby(['parent_foodcode', 'parent_desc', 'ingred_code', 'ingred_desc'])['ingred_wt'].agg(sum).reset_index()

# -*- coding: utf-8 -*-
"""
Created on Sat Apr  1 20:21:57 2023

@author: Trevor Chan
"""

FILENAME = '../data/01/fndds_18_all_ingredients.csv'

def main():   
    df = pd.read_csv(FILENAME)  # Replace with the appropriate file path
    df = preprocessing(df) 
    
    all_foodcodes = df[df.columns[0]].unique()
    foodcode_ingredients = {}
    
    for foodcode in all_foodcodes:
        parent_desc = df.loc[df['parent_foodcode'] == foodcode, 'parent_desc'].iloc[0]
        ingredients_proportions = get_ingredient_proportions(df, foodcode)
        foodcode_ingredients[foodcode] = {"parent_desc": parent_desc, "ingredients": ingredients_proportions}
    
    aggregated_data = []
    for foodcode, data in foodcode_ingredients.items():
        for ingred_code, ingred_data in data['ingredients'].items():
            aggregated_data.append({
                'parent_foodcode': foodcode,
                'parent_desc': data['parent_desc'],
                'ingred_code': ingred_code,
                'ingred_desc': ingred_data['ingred_desc'],
                'ingred_wt': ingred_data['ingred_wt']
            })
    
    # Convert the list of dictionaries to a dataframe
    aggregated_18 = pd.DataFrame(aggregated_data)
    aggregated_18.to_csv('../data/02/fndds_18_ingredient_wt_corrected.csv', sep=',', encoding='utf-8', index=False)


main()

aggregated_18 = pd.read_csv('../data/02/fndds_18_ingredient_wt_corrected.csv')

fndds18 = pd.read_csv('../data/01/fndds_18_ingredient_codes.csv', usecols=['Food code', 'Main food description', 'Ingredient code', 'Ingredient description', 'Ingredient weight (g)'])
fndds18.rename(columns={'Food code': 'parent_foodcode', 'Main food description': 'parent_desc', 'Ingredient code': 'ingred_code', 'Ingredient description': 'ingred_desc', 'Ingredient weight (g)': 'ingred_wt'}, inplace=True)
fndds18 = fndds18[fndds18['ingred_code']<10000000]
fndds18_combined = pd.concat([aggregated_18, fndds18], ignore_index=True).drop_duplicates(keep='first')
fndds18_combined = fndds18_combined.groupby(['parent_foodcode', 'parent_desc', 'ingred_code', 'ingred_desc'])['ingred_wt'].agg(sum).reset_index()

fndds16_diff = fndds16_combined[~fndds16_combined['parent_foodcode'].isin(fndds18_combined['parent_foodcode'])]

fndds18_diff = fndds18_combined[~fndds18_combined['parent_foodcode'].isin(fndds16_combined['parent_foodcode'])]

fndds_16_18_all = pd.concat([fndds16_diff, fndds18_combined])

fndds_16_18_all.loc[-1] = [83208000, 'Coleslaw dressing, light', 42230, 'Salad Dressing, coleslaw, reduced fat', 100] # code missing from data added in

fndds_16_18_all.to_csv('../data/02/fndds_16_18_all.csv', index=None)