# Import packages
import pandas as pd
import requests

url16, url18 = 'https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2015-2016%20FNDDS%20At%20A%20Glance%20-%20FNDDS%20Ingredients.xlsx', 'https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2017-2018%20FNDDS%20At%20A%20Glance%20-%20FNDDS%20Ingredients.xlsx'

header = {
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"
}

r16 = requests.get(url16, headers=header)
r18 = requests.get(url18, headers=header)

fndds_16 = pd.ExcelFile(r16.content, engine='openpyxl').parse('FNDDS Ingredients', header = 1)
fndds_18 = pd.ExcelFile(r18.content, engine='openpyxl').parse('FNDDS Ingredients', header = 1)

# Replace codes
# fndds_16.replace({'Ingredient code': 11100000}, 1111, inplace=True)
# fndds_16.replace({'Ingredient description': 'Milk, NFS'}, 'Milk, averaged fat, with added vitamin A and D', inplace=True)

# fndds_16.replace({'Ingredient code': 81200100}, 4321, inplace=True)
# fndds_16.replace({'Ingredient description': 'Oil or table fat, NFS'}, 'Oil, table fat, averaged', inplace=True)

# fndds_16.replace({'Ingredient code': 21500000}, 23222, inplace=True)
# fndds_16.replace({'Ingredient description': 'Ground beef, raw'}, 'Ground beef, raw, averaged', inplace=True)

# fndds_16.replace({'Ingredient code': 21500100}, 23223, inplace=True)
# fndds_16.replace({'Ingredient description': 'Ground beef, cooked'}, 'Ground beef, cooked, averaged', inplace=True)

# fndds_16.replace({'Ingredient code': 82101000}, 4322, inplace=True)
# fndds_16.replace({'Ingredient description': 'Vegetable oil, NFS'}, 'Vegetable oil, averaged', inplace=True)

# fndds_16.replace({'Ingredient code': 81100000}, 4323, inplace=True)
# fndds_16.replace({'Ingredient description': 'Table fat, NFS'}, 'Table fat, averaged', inplace=True)

# fndds_16.loc[fndds_16['Food code'].isin([11100000])] = fndds_16.loc[fndds_16['Food code'].isin([11100000])].drop_duplicates(subset='Food code')
# fndds_16.loc[fndds_16['Food code'].isin([81200100])] = fndds_16.loc[fndds_16['Food code'].isin([81200100])].drop_duplicates(subset='Food code')
# fndds_16.loc[fndds_16['Food code'].isin([21500000])] = fndds_16.loc[fndds_16['Food code'].isin([21500000])].drop_duplicates(subset='Food code')
# fndds_16.loc[fndds_16['Food code'].isin([21500100])] = fndds_16.loc[fndds_16['Food code'].isin([21500100])].drop_duplicates(subset='Food code')
# fndds_16.loc[fndds_16['Food code'].isin([82101000])] = fndds_16.loc[fndds_16['Food code'].isin([82101000])].drop_duplicates(subset='Food code')
# fndds_16.loc[fndds_16['Food code'].isin([81100000])] = fndds_16.loc[fndds_16['Food code'].isin([81100000])].drop_duplicates(subset='Food code')

# fndds_16 = fndds_16.dropna()

# fndds_16.loc[fndds_16['Food code'] == 11100000, 'Ingredient code'] = 1111
# fndds_16.loc[fndds_16['Food code'] == 11100000, 'Ingredient description'] = 'Milk, averaged fat, with added vitamin A and D'

# fndds_16.loc[fndds_16['Food code'] == 81200100, 'Ingredient code'] = 4321
# fndds_16.loc[fndds_16['Food code'] == 81200100, 'Ingredient description'] = 'Oil, table fat, averaged'

# fndds_16.loc[fndds_16['Food code'] == 21500000, 'Ingredient code'] = 23222
# fndds_16.loc[fndds_16['Food code'] == 21500000, 'Ingredient description'] = 'Ground beef, raw, averaged'

# fndds_16.loc[fndds_16['Food code'] == 21500100, 'Ingredient code'] = 23223
# fndds_16.loc[fndds_16['Food code'] == 21500100, 'Ingredient description'] = 'Ground beef, cooked, averaged'

# fndds_16.loc[fndds_16['Food code'] == 82101000, 'Ingredient code'] = 4322
# fndds_16.loc[fndds_16['Food code'] == 82101000, 'Ingredient description'] = 'Vegetable oil, averaged'

# fndds_16.loc[fndds_16['Food code'] == 81100000, 'Ingredient code'] = 4323
# fndds_16.loc[fndds_16['Food code'] == 81100000, 'Ingredient description'] = 'Table fat, averaged'

# # FNDDS 17-18
# fndds_18.replace({'Ingredient code': 11100000}, 1111, inplace=True)
# fndds_18.replace({'Ingredient description': 'Milk, NFS'}, 'Milk, averaged fat, with added vitamin A and D', inplace=True)

# fndds_18.replace({'Ingredient code': 81200100}, 4321, inplace=True)
# fndds_18.replace({'Ingredient description': 'Oil or table fat, NFS'}, 'Oil, table fat, averaged', inplace=True)

# fndds_18.replace({'Ingredient code': 21500000}, 23222, inplace=True)
# fndds_18.replace({'Ingredient description': 'Ground beef, raw'}, 'Ground beef, raw, averaged', inplace=True)

# fndds_18.replace({'Ingredient code': 21500100}, 23223, inplace=True)
# fndds_18.replace({'Ingredient description': 'Ground beef, cooked'}, 'Ground beef, cooked, averaged', inplace=True)

# fndds_18.replace({'Ingredient code': 82101000}, 4322, inplace=True)
# fndds_18.replace({'Ingredient description': 'Vegetable oil, NFS'}, 'Vegetable oil, averaged', inplace=True)

# fndds_18.replace({'Ingredient code': 81100000}, 4323, inplace=True)
# fndds_18.replace({'Ingredient description': 'Table fat, NFS'}, 'Table fat, averaged', inplace=True)

# fndds_18.loc[fndds_18['Food code'].isin([11100000])] = fndds_18.loc[fndds_18['Food code'].isin([11100000])].drop_duplicates(subset='Food code')
# fndds_18.loc[fndds_18['Food code'].isin([81200100])] = fndds_18.loc[fndds_18['Food code'].isin([81200100])].drop_duplicates(subset='Food code')
# fndds_18.loc[fndds_18['Food code'].isin([21500000])] = fndds_18.loc[fndds_18['Food code'].isin([21500000])].drop_duplicates(subset='Food code')
# fndds_18.loc[fndds_18['Food code'].isin([21500100])] = fndds_18.loc[fndds_18['Food code'].isin([21500100])].drop_duplicates(subset='Food code')
# fndds_18.loc[fndds_18['Food code'].isin([82101000])] = fndds_18.loc[fndds_18['Food code'].isin([82101000])].drop_duplicates(subset='Food code')
# fndds_18.loc[fndds_18['Food code'].isin([81100000])] = fndds_18.loc[fndds_18['Food code'].isin([81100000])].drop_duplicates(subset='Food code')

# fndds_18 = fndds_18.dropna()

# fndds_18.loc[fndds_18['Food code'] == 11100000, 'Ingredient code'] = 1111
# fndds_18.loc[fndds_18['Food code'] == 11100000, 'Ingredient description'] = 'Milk, averaged fat, with added vitamin A and D'

# fndds_18.loc[fndds_18['Food code'] == 81200100, 'Ingredient code'] = 4321
# fndds_18.loc[fndds_18['Food code'] == 81200100, 'Ingredient description'] = 'Oil, table fat, averaged'

# fndds_18.loc[fndds_18['Food code'] == 21500000, 'Ingredient code'] = 23222
# fndds_18.loc[fndds_18['Food code'] == 21500000, 'Ingredient description'] = 'Ground beef, raw, averaged'

# fndds_18.loc[fndds_18['Food code'] == 21500100, 'Ingredient code'] = 23223
# fndds_18.loc[fndds_18['Food code'] == 21500100, 'Ingredient description'] = 'Ground beef, cooked, averaged'

# fndds_18.loc[fndds_18['Food code'] == 82101000, 'Ingredient code'] = 4322
# fndds_18.loc[fndds_18['Food code'] == 82101000, 'Ingredient description'] = 'Vegetable oil, averaged'

# fndds_18.loc[fndds_18['Food code'] == 81100000, 'Ingredient code'] = 4323
# fndds_18.loc[fndds_18['Food code'] == 81100000, 'Ingredient description'] = 'Table fat, averaged'

# Write files (will be used to gather ingredient codes that were not ingredientized in following script)
fndds_16.to_csv('../data/01/fndds_16_ingredient_codes.csv', index=None)
fndds_18.to_csv('../data/01/fndds_18_ingredient_codes.csv', index=None)

# Part 2: iteratively extract ingredient codes from 8-digit foodcodes
fndds_16.rename(columns={'Food code':'foodcode'}, inplace=True)

fndds16_ingred_1 = fndds_16[fndds_16['Ingredient code']>10000000].copy()
fndds16_ingred_1.rename(columns={'foodcode':'parent_foodcode', 'Main food description': 'parent_desc', 'Ingredient code': 'foodcode', 'Ingredient description': 'ingred_desc1', 'Ingredient weight': 'ingred_wt1'}, inplace=True)
fndds16_ingred_1 = fndds16_ingred_1[['parent_foodcode', 'parent_desc', 'foodcode', 'ingred_desc1', 'ingred_wt1']]

fndds16_ingred_2 = pd.merge(fndds16_ingred_1, fndds_16, how='left', on='foodcode')
fndds16_ingred_2.rename(columns={'foodcode':'ingred_code1', 'Ingredient code': 'foodcode', 'Ingredient description': 'ingred_desc2', 'Ingredient weight': 'ingred_wt2'}, inplace=True)
fndds16_ingred_2 = fndds16_ingred_2[['parent_foodcode', 'parent_desc', 'foodcode', 'ingred_code1', 'ingred_desc1', 'ingred_wt1', 'ingred_desc2', 'ingred_wt2']]

fndds16_ingred_3 = pd.merge(fndds16_ingred_2, fndds_16, how='left', on='foodcode')
fndds16_ingred_3.rename(columns={'foodcode':'ingred_code2', 'Ingredient code': 'foodcode', 'Ingredient description': 'ingred_desc3', 'Ingredient weight': 'ingred_wt3'}, inplace=True)
fndds16_ingred_3 = fndds16_ingred_3[['parent_foodcode', 'parent_desc', 'foodcode', 'ingred_code1', 'ingred_desc1', 'ingred_wt1', 'ingred_code2', 'ingred_desc2', 'ingred_wt2', 'ingred_desc3', 'ingred_wt3']]

fndds16_ingred_4 = pd.merge(fndds16_ingred_3, fndds_16, how='left', on='foodcode')
fndds16_ingred_4.rename(columns={'foodcode':'ingred_code3', 'Ingredient code': 'ingred_code4', 'Ingredient description': 'ingred_desc4', 'Ingredient weight': 'ingred_wt4'}, inplace=True)
fndds16_ingred_4 = fndds16_ingred_4[['parent_foodcode', 'parent_desc', 'ingred_code1', 'ingred_desc1', 'ingred_wt1', 'ingred_code2', 'ingred_desc2', 'ingred_wt2', 'ingred_code3', 'ingred_desc3', 'ingred_wt3', 'ingred_code4', 'ingred_desc4', 'ingred_wt4']]

fndds_18.rename(columns={'Food code':'foodcode', 'Ingredient weight (g)': 'Ingredient weight'}, inplace=True)

fndds18_ingred_1 = fndds_18[fndds_18['Ingredient code']>10000000].copy()
fndds18_ingred_1.rename(columns={'foodcode':'parent_foodcode', 'Main food description': 'parent_desc', 'Ingredient code': 'foodcode', 'Ingredient description': 'ingred_desc1', 'Ingredient weight': 'ingred_wt1'}, inplace=True)
fndds18_ingred_1 = fndds18_ingred_1[['parent_foodcode', 'parent_desc', 'foodcode', 'ingred_desc1', 'ingred_wt1']]

fndds18_ingred_2 = pd.merge(fndds18_ingred_1, fndds_18, how='left', on='foodcode')
fndds18_ingred_2.rename(columns={'foodcode':'ingred_code1', 'Ingredient code': 'foodcode', 'Ingredient description': 'ingred_desc2', 'Ingredient weight': 'ingred_wt2'}, inplace=True)
fndds18_ingred_2 = fndds18_ingred_2[['parent_foodcode', 'parent_desc', 'foodcode', 'ingred_code1', 'ingred_desc1', 'ingred_wt1', 'ingred_desc2', 'ingred_wt2']]

fndds18_ingred_3 = pd.merge(fndds18_ingred_2, fndds_18, how='left', on='foodcode')
fndds18_ingred_3.rename(columns={'foodcode':'ingred_code2', 'Ingredient code': 'foodcode', 'Ingredient description': 'ingred_desc3', 'Ingredient weight': 'ingred_wt3'}, inplace=True)
fndds18_ingred_3 = fndds18_ingred_3[['parent_foodcode', 'parent_desc', 'foodcode', 'ingred_code1', 'ingred_desc1', 'ingred_wt1', 'ingred_code2', 'ingred_desc2', 'ingred_wt2', 'ingred_desc3', 'ingred_wt3']]

fndds18_ingred_4 = pd.merge(fndds18_ingred_3, fndds_18, how='left', on='foodcode')
fndds18_ingred_4.rename(columns={'foodcode':'ingred_code3', 'Ingredient code': 'ingred_code4', 'Ingredient description': 'ingred_desc4', 'Ingredient weight': 'ingred_wt4'}, inplace=True)
fndds18_ingred_4 = fndds18_ingred_4[['parent_foodcode', 'parent_desc', 'ingred_code1', 'ingred_desc1', 'ingred_wt1', 'ingred_code2', 'ingred_desc2', 'ingred_wt2', 'ingred_code3', 'ingred_desc3', 'ingred_wt3', 'ingred_code4', 'ingred_desc4', 'ingred_wt4']]

fndds16_ingred_4.to_csv('../data/01/fndds_16_all_ingredients.csv', index=None)
fndds18_ingred_4.to_csv('../data/01/fndds_18_all_ingredients.csv', index=None)

# Part 3: Consolidate ingredient codes for nutrient values data
# Load data for FNDDS ingredient values
url_n16, url_n18 = 'https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2015-2016%20FNDDS%20At%20A%20Glance%20-%20Ingredient%20Nutrient%20Values.xlsx', 'https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2017-2018%20FNDDS%20At%20A%20Glance%20-%20Ingredient%20Nutrient%20Values.xlsx'

r_n16 = requests.get(url_n16, headers=header)
r_n18 = requests.get(url_n18, headers=header)

nutrient_values_16 = pd.ExcelFile(r_n16.content, engine='openpyxl').parse('Ingredient Nutrient Values', header = 1)
nutrient_values_18 = pd.ExcelFile(r_n18.content, engine='openpyxl').parse('Ingredient Nutrient Values', header = 1)

nutrient_values_16.rename(columns={'SR description': 'Ingredient description'}, inplace=True)
nutrient_values_18.rename(columns={'SR description': 'Ingredient description'}, inplace=True)

# milk_nfs = nutrient_values_16.loc[nutrient_values_16['Ingredient code'].isin([1077, 1079, 1082, 1085])].copy()

# milk_nfs['average_nutrients'] = milk_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
# milk_nfs = milk_nfs.drop_duplicates(subset='Nutrient description')
# milk_nfs.drop(columns={'Nutrient value'}, inplace=True)
# milk_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
# milk_nfs['Ingredient code'] = 1111
# milk_nfs['Ingredient description'] = 'Milk, averaged fat, with added vitamin A and D'

# oil_table_fat_nfs = nutrient_values_16.loc[nutrient_values_16['Ingredient code'].isin([4613, 1001, 4694, 4044, 4518, 4582, 4053])].copy()

# oil_table_fat_nfs['average_nutrients'] = oil_table_fat_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
# oil_table_fat_nfs = oil_table_fat_nfs.drop_duplicates(subset='Nutrient description')
# oil_table_fat_nfs.drop(columns={'Nutrient value'}, inplace=True)
# oil_table_fat_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
# oil_table_fat_nfs['Ingredient code'] = 4321
# oil_table_fat_nfs['Ingredient description'] = 'Oil, table fat, averaged'

# vegetable_oil_nfs = nutrient_values_16.loc[nutrient_values_16['Ingredient code'].isin([4044, 4518, 4582, 4053])].copy()

# vegetable_oil_nfs['average_nutrients'] = vegetable_oil_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
# vegetable_oil_nfs = vegetable_oil_nfs.drop_duplicates(subset='Nutrient description')
# vegetable_oil_nfs.drop(columns={'Nutrient value'}, inplace=True)
# vegetable_oil_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
# vegetable_oil_nfs['Ingredient code'] = 4322
# vegetable_oil_nfs['Ingredient description'] = 'Vegetable oil, averaged'

# table_fat_nfs = nutrient_values_16.loc[nutrient_values_16['Ingredient code'].isin([1001, 4613, 4694])].copy()

# table_fat_nfs['average_nutrients'] = table_fat_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
# table_fat_nfs = table_fat_nfs.drop_duplicates(subset='Nutrient description')
# table_fat_nfs.drop(columns={'Nutrient value'}, inplace=True)
# table_fat_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
# table_fat_nfs['Ingredient code'] = 4323
# table_fat_nfs['Ingredient description'] = 'Table fat, averaged'

# ground_beef_raw = nutrient_values_16.loc[nutrient_values_16['Ingredient code'].isin([23567, 23572, 23577, 23562, 23557])].copy()

# ground_beef_raw['average_nutrients'] = ground_beef_raw.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
# ground_beef_raw = ground_beef_raw.drop_duplicates(subset='Nutrient description')
# ground_beef_raw.drop(columns={'Nutrient value'}, inplace=True)
# ground_beef_raw.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
# ground_beef_raw['Ingredient code'] = 23222
# ground_beef_raw['Ingredient description'] = 'Ground beef, raw, averaged'

# ground_beef_cooked = nutrient_values_16.loc[nutrient_values_16['Ingredient code'].isin([23578, 23573, 23568, 23563, 2047])].copy()

# ground_beef_cooked['average_nutrients'] = ground_beef_cooked.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
# ground_beef_cooked = ground_beef_cooked.drop_duplicates(subset='Nutrient description')
# ground_beef_cooked.drop(columns={'Nutrient value'}, inplace=True)
# ground_beef_cooked.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
# ground_beef_cooked['Ingredient code'] = 23223
# ground_beef_cooked['Ingredient description'] = 'Ground beef, cooked, averaged'

# nutrient_values_16 = pd.concat([nutrient_values_16, milk_nfs, oil_table_fat_nfs, vegetable_oil_nfs, table_fat_nfs, ground_beef_raw, ground_beef_cooked])

nutrients_16 = pd.pivot(nutrient_values_16, index=['Ingredient code', 'Ingredient description'], columns='Nutrient description', values='Nutrient value')

nutrients_16.reset_index(inplace=True)

nutrients_16.rename(columns={'4:0': 'Butyric acid', '6:0': 'Caproic acid', '8:0': 'Caprylic acid', '10:0': 'Capric acid', '12:0': 'Lauric acid', '14:0': 'Myristic acid', '16:0': 'Palmitic acid', '16:1': 'Palmitoleic acid', '18:0': 'Stearic acid', '18:1': 'Oleic acid', '18:2': 'Linoleic acid', '18:3': 'Linolenic acid', '18:4': 'Stearidonic acid', '20:1': 'Eicosenoic acid', '20:4': 'Arachidonic acid', '20:5 n-3': 'Eicosapentaenoic acid', '22:1': 'Erucic acid', '22:5 n-3': 'Docosapentaenoic acid', '22:6 n-3': 'Docosahexaenoic acid'}, inplace=True)

# milk_nfs = nutrient_values_18.loc[nutrient_values_18['Ingredient code'].isin([1077, 1079, 1082, 1085])].copy()

# milk_nfs['average_nutrients'] = milk_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
# milk_nfs = milk_nfs.drop_duplicates(subset='Nutrient description')
# milk_nfs.drop(columns={'Nutrient value'}, inplace=True)
# milk_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
# milk_nfs['Ingredient code'] = 1111
# milk_nfs['Ingredient description'] = 'Milk, averaged fat, with added vitamin A and D'

# oil_table_fat_nfs = nutrient_values_18.loc[nutrient_values_18['Ingredient code'].isin([4613, 1001, 4694, 4044, 4518, 4582, 4053])].copy()

# oil_table_fat_nfs['average_nutrients'] = oil_table_fat_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
# oil_table_fat_nfs = oil_table_fat_nfs.drop_duplicates(subset='Nutrient description')
# oil_table_fat_nfs.drop(columns={'Nutrient value'}, inplace=True)
# oil_table_fat_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
# oil_table_fat_nfs['Ingredient code'] = 4321
# oil_table_fat_nfs['Ingredient description'] = 'Oil, table fat, averaged'

# table_fat_nfs = nutrient_values_18.loc[nutrient_values_18['Ingredient code'].isin([1001, 4613, 4694])].copy()

# table_fat_nfs['average_nutrients'] = table_fat_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
# table_fat_nfs = table_fat_nfs.drop_duplicates(subset='Nutrient description')
# table_fat_nfs.drop(columns={'Nutrient value'}, inplace=True)
# table_fat_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
# table_fat_nfs['Ingredient code'] = 4323
# table_fat_nfs['Ingredient description'] = 'Table fat, averaged'

# ground_beef_raw = nutrient_values_18.loc[nutrient_values_18['Ingredient code'].isin([23567, 23572, 23577, 23562, 23557])].copy()

# ground_beef_raw['average_nutrients'] = ground_beef_raw.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
# ground_beef_raw = ground_beef_raw.drop_duplicates(subset='Nutrient description')
# ground_beef_raw.drop(columns={'Nutrient value'}, inplace=True)
# ground_beef_raw.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
# ground_beef_raw['Ingredient code'] = 23222
# ground_beef_raw['Ingredient description'] = 'Ground beef, raw, averaged'

# ground_beef_cooked = nutrient_values_18.loc[nutrient_values_18['Ingredient code'].isin([23578, 23573, 23568, 23563, 2047])].copy()

# ground_beef_cooked['average_nutrients'] = ground_beef_cooked.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
# ground_beef_cooked = ground_beef_cooked.drop_duplicates(subset='Nutrient description')
# ground_beef_cooked.drop(columns={'Nutrient value'}, inplace=True)
# ground_beef_cooked.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
# ground_beef_cooked['Ingredient code'] = 23223
# ground_beef_cooked['Ingredient description'] = 'Ground beef, cooked, averaged'

# nutrient_values_18 = pd.concat([nutrient_values_18, milk_nfs, oil_table_fat_nfs, vegetable_oil_nfs, table_fat_nfs, ground_beef_raw, ground_beef_cooked])

nutrients_18 = pd.pivot(nutrient_values_18, index=['Ingredient code', 'Ingredient description'], columns='Nutrient description', values='Nutrient value')

nutrients_18.reset_index(inplace=True)

nutrients_18.rename(columns={'4:0': 'Butyric acid', '6:0': 'Caproic acid', '8:0': 'Caprylic acid', '10:0': 'Capric acid', '12:0': 'Lauric acid', '14:0': 'Myristic acid', '16:0': 'Palmitic acid', '16:1': 'Palmitoleic acid', '18:0': 'Stearic acid', '18:1': 'Oleic acid', '18:2': 'Linoleic acid', '18:3': 'Linolenic acid', '18:4': 'Stearidonic acid', '20:1': 'Eicosenoic acid', '20:4': 'Arachidonic acid', '20:5 n-3': 'Eicosapentaenoic acid', '22:1': 'Erucic acid', '22:5 n-3': 'Docosapentaenoic acid', '22:6 n-3': 'Docosahexaenoic acid'}, inplace=True)

pd.concat([nutrients_16, nutrients_18], axis=0).drop_duplicates(subset='Ingredient code').to_csv('../data/01/fndds_all_ingredient_nutrient_values.csv', index=None)
