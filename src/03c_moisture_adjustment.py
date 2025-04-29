import pandas as pd

#load FNDDS moisture change files
fndds2 = pd.read_csv('../data/04/fndds_crosswalk/fnddsmoistureadjustment/fndds2_moisturefatadj.txt', sep='^', header=None)
fndds3 = pd.read_csv('../data/04/fndds_crosswalk/fnddsmoistureadjustment/fndds3_MoistNFatAdjust.txt', sep='^', header=None)
fndds4 = pd.read_csv('../data/04/fndds_crosswalk/fnddsmoistureadjustment/fndds4_MoistNFatAdjust.txt', sep='^', header=None)
fndds5 = pd.read_csv('../data/04/fndds_crosswalk/fnddsmoistureadjustment/fndds5_MoistNFatAdjust.txt', sep='^', header=None)
fndds2011 = pd.read_csv('../data/04/fndds_crosswalk/fnddsmoistureadjustment/fndds2011_MoistNFatAdjust.txt', sep='^', header=None)
fndds2013 = pd.read_csv('../data/04/fndds_crosswalk/fnddsmoistureadjustment/fndds2013_MoistNFatAdjust.csv')
fndds2015 = pd.read_csv('../data/04/fndds_crosswalk/fnddsmoistureadjustment/fndds2015_moisture.csv')
fndds2017 = pd.read_csv('../data/04/fndds_crosswalk/fnddsmoistureadjustment/fndds2017_moisture.csv')

#load fndds_fcid ingredients file to add moisture change values
fndds_fcid_all = pd.read_csv('../data/03b/fndds_fcid_all.csv')

# parse files and combine
fndds2 = fndds2[[0, 3]].rename(columns={0:'parent_foodcode', 3:'moisture_change'})
fndds3 = fndds3[[0, 3]].rename(columns={0:'parent_foodcode', 3:'moisture_change'})
fndds4 = fndds4[[0, 3]].rename(columns={0:'parent_foodcode', 3:'moisture_change'})
fndds5 = fndds5[[0, 3]].rename(columns={0:'parent_foodcode', 3:'moisture_change'})
fndds2011 = fndds2011[[0, 3]].rename(columns={0:'parent_foodcode', 3:'moisture_change'})
fndds2013 = fndds2013[['Food code', 'Moisture change']].rename(columns={'Food code':'parent_foodcode', 'Moisture change':'moisture_change'})
fndds2015 = fndds2015[['Food code', 'Moisture change (%)']].rename(columns={'Food code':'parent_foodcode', 'Moisture change (%)':'moisture_change'})
fndds2017 = fndds2017[['Food code', 'Moisture change (%)']].rename(columns={'Food code':'parent_foodcode', 'Moisture change (%)':'moisture_change'})

fndds_water = pd.concat([fndds2017, fndds2015, fndds2013, fndds2011, fndds5, fndds4, fndds3, fndds2]).drop_duplicates(subset='parent_foodcode')

missing = fndds_fcid_all[~fndds_fcid_all['parent_foodcode'].isin(fndds_water['parent_foodcode'])]

added_codes = pd.read_csv('../data/03/manually_curated/string_match_discontinued_complete_with_annotation.csv')

added_codes = added_codes.rename(columns={'Unnamed: 5':'note'})
added_codes = added_codes.dropna(subset='note')

codes_to_match = added_codes[['DRXFDCD']].rename(columns={'DRXFDCD':'parent_foodcode'})
matched = codes_to_match.merge(fndds_water, on='parent_foodcode')
matched = matched.rename(columns={'parent_foodcode':'DRXFDCD'})
added_codes = added_codes[['DRXFDCD', 'parent_foodcode']]

moisture_values = matched.merge(added_codes, on='DRXFDCD').drop(columns='DRXFDCD')
moisture_replace = missing.merge(moisture_values, on='parent_foodcode')

fndds_fcid_all_missing = fndds_fcid_all[~fndds_fcid_all['parent_foodcode'].isin(missing['parent_foodcode'])]
fndds_fcid_all_moisture_1 = fndds_fcid_all_missing.merge(fndds_water, on='parent_foodcode')
fndds_fcid_all_moisture_2 = pd.concat([fndds_fcid_all_moisture_1, moisture_replace])
fndds_fcid_all_moisture_2.isna().sum() # one foodcode 27118140 with missing moisture change, replace with the similar foodcode 27218110 # 2 NAs found for tomato, dried, replace with tomato, sun-dried

fndds_fcid_all_moisture_2['moisture_change'].fillna(-47.3,inplace=True)
fndds_fcid_all_moisture_2['ingred_code'].fillna(11955, inplace=True)

water = pd.read_csv('../data/01/fndds_all_ingredient_nutrient_values.csv', usecols=['Ingredient code', 'Water'])

water = water.rename(columns={'Ingredient code':'ingred_code'})
fndds_fcid_all_moisture_3 = fndds_fcid_all_moisture_2.merge(water, on='ingred_code')
fndds_fcid_all_moisture_3['Water Change'] = fndds_fcid_all_moisture_3['Water'] * (fndds_fcid_all_moisture_3['moisture_change'] / 100)
fndds_fcid_all_moisture_3['Corrected Weight'] = fndds_fcid_all_moisture_3['ingred_wt'] + (fndds_fcid_all_moisture_3['ingred_wt'] * (fndds_fcid_all_moisture_3['Water Change']/100))
fndds_fcid_all_moisture_3 = fndds_fcid_all_moisture_3[['parent_foodcode', 'parent_desc', 'ingred_code', 'ingred_desc', 'Corrected Weight']]
fndds_fcid_all_moisture_3.rename(columns={'Corrected Weight':'ingred_wt'},inplace=True)

fndds_fcid_all_moisture_3.to_csv('../data/03c/fndds_fcid_all_moisture_corrected.csv', index= None)