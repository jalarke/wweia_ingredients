# Import packages
# Edit 062424: Commented out line 80 'Spices, other' : 'Spices, pepper, black'. Spices other are undefined and should not be assumed as black pepper.
import pandas as pd
import string
import re
from polyfuzz.models import TFIDF
from polyfuzz import PolyFuzz
import nltk
nltk.download(['wordnet', 'omw-1.4'])
wn = nltk.WordNetLemmatizer()

##PART 1:
# Load data
fcid = pd.read_csv('../data/03b/fcid_data.csv', usecols=['Food Description', 'FCID Description'])
ingred = pd.read_csv('../data/03b/ingredient_codes_to_ingredientization.csv') # list of ingredients needing further disaggregation
fcid = fcid.dropna()

punct = string.punctuation[0:11] + string.punctuation[13:] # remove '-' from the list of punctuation. This is needed for the text cleaner in the following cell
stopwords = ['', '1', '2', '3', '4', '5' ,'100','and', 'to', 'not', 'no', 'high', 'added', 'vitamin', 'A', 'D', 'oz', 'in', 'with', 'or', 'only', 'cooking', 'as', 'food', 'distribution', 'form', 'W', 'WO', 'NS', 'NFS', 'INCL']

def clean_text(text):
    text = "".join([word for word in text if word not in punct])
    tokens = re.split('[-\W+]', text)
    text = [word for word in tokens if word not in stopwords]
    text = [wn.lemmatize(word) for word in tokens if word not in stopwords]
    return ' '.join(text)

fcid['fcid_clean'] = fcid['Food Description'].apply(lambda x: clean_text(x))
ingred['ingred_clean'] = ingred['Main.food.description'].apply(lambda x: clean_text(x))

fcid_list = fcid['fcid_clean'].to_list()
ingred_list = ingred['ingred_clean'].to_list()

tfidf = TFIDF(n_gram_range=(3, 3))
model = PolyFuzz(tfidf).match(ingred_list, fcid_list)

match = model.get_matches()
match.rename(columns={'From':'ingred_clean', 'To':'fcid_clean'},inplace=True)
fcid_matched = match.merge(fcid, on='fcid_clean', how='left')
fcid_matched_ = fcid_matched.merge(ingred, on='ingred_clean', how='left').drop_duplicates(subset='ingred_clean')

match_merge = match.merge(ingred, on='ingred_clean', how='left')
match_merge_ = match_merge.merge(fcid, on='fcid_clean', how='left').drop_duplicates(subset='Ingredient code')
match_merge_[['Main.food.description', 'Food Description', 'ingred_clean', 'fcid_clean', 'Ingredient code', 'Similarity']].sort_values('Similarity', ascending=False).to_csv('../data/03b/fcid_match.csv', index=None)

##PART 2:
fcid = pd.read_csv('../data/03b/fcid_data.csv')
matches = pd.read_csv('../data/03b/manually_curated/fcid_match_complete_edit_0625.csv') # load manually curated data, updated 062524 to not disaggregate chicken/beef broths or gravies. These break down into small amounts of meat and animal fats that aren't appropriate for vegetarian recipes.

#fcid = fcid.drop([122397, 122401, 122404]) # drop beef products from vegetable soup (75649010)
fcid = fcid[fcid['Modification Code']==0]
fcid = fcid[['WWEIA Food Code', 'Food Description', 'FCID Description', 'Commodity Weight']]

corrected = matches[matches['correct'] == 'n']
corrected = corrected.drop(columns=['ingred_clean', 'fcid_clean', 'Similarity', 'correct'])

matched = matches[~matches['Ingredient code'].isin(corrected['Ingredient code'])]
matched = matched.merge(fcid, on='Food Description', how='left')
matched = matched[['Main.food.description', 'Ingredient code', 'FCID Description', 'Commodity Weight']]

fcid_corrected = corrected.merge(fcid, on='WWEIA Food Code', how='left')
fcid_corrected = fcid_corrected[['Main.food.description', 'Ingredient code', 'FCID Description', 'Commodity Weight']]

# some ingredients in the FCID are not found in FNDDS. Selecting the most appropriate matches. 
fcid_ingred = pd.concat([matched, fcid_corrected])
fcid_ingred['FCID Description'].replace({'Sugarcane, sugar': 'Sugars, granulated',
                                         'Beet, sugar': 'Sugars, granulated',
                                         'Soybean, oil': 'Oil, soybean, salad or cooking',
                                         'Wheat, flour' : 'Wheat flour, white, all-purpose, enriched, bleached',
                                         'Milk, nonfat solids' : 'Milk, reduced fat, fluid, 2% milkfat, with added vitamin A and vitamin D',
                                         'Milk, fat' : 'Milk, reduced fat, fluid, 2% milkfat, with added vitamin A and vitamin D',
                                         'Milk, water': 'Milk, reduced fat, fluid, 2% milkfat, with added vitamin A and vitamin D',
                                         'Corn, field, syrup': 'Syrups, corn, light',
                                         'Cottonseed, oil' : 'Oil, cottonseed, salad or cooking',
                                         'Rapeseed, oil' : 'Oil, canola',
                                         'Sunflower, oil' : 'Oil, sunflower, linoleic, (approx. 65%)',
                                         'Safflower, oil' : 'Oil, safflower, salad or cooking, high oleic (primary safflower oil of commerce)',
                                         'Corn, field, oil': 'Oil, corn, industrial and retail, all purpose salad or cooking',
                                         'Water, indirect, all sources': 'Beverages, water, tap, drinking',
                                         'Corn, field, starch' : 'Cornstarch',
                                         'Egg, whole' : 'Egg, whole, raw, fresh',
                                         'Beef, fat' : 'Beef, retail cuts, separable fat, cooked',
                                         #'Spices, other' : 'Spices, pepper, black',
                                         'Beef, meat byproducts' : 'Beef, ground, 75% lean meat / 25% fat, patty, cooked, broiled',
                                         'Rice, flour' : 'Rice flour, white, unenriched',
                                         'Onion, bulb' : 'Onions, raw',
                                         'Potato, flour' : 'Potato flour',
                                         'Oat, groats/rolled oats' : 'Cereals, oats, regular and quick, not fortified, dry',
                                         'Garlic, bulb' : 'Garlic, raw',
                                         'Cassava' : 'Cassava, raw',
                                         'Beef, meat' : 'Beef, ground, 75% lean meat / 25% fat, patty, cooked, broiled',
                                         'Barley, flour' : 'Barley, pearled, raw',
                                         'Onion, bulb, dried' : 'Spices, onion powder',
                                         #'Honey'
                                         'Tomato' : 'Tomatoes, red, ripe, raw, year round average',
                                         'Peanut, oil' : 'Oil, peanut, salad or cooking',
                                         'Olive, oil' : 'Oil, olive, salad or cooking',
                                         'Sesame, oil' : 'Oil, sesame, salad or cooking',
                                         'Soybean, flour' : 'Soy flour, defatted',
                                         'Chicken, fat' : 'Fat, chicken',
                                         'Chicken, meat' : 'Chicken, broiler, rotisserie, BBQ, breast, meat only',
                                         'Pork, fat' : 'Lard',
                                         'Wheat, grain' : 'Wheat flour, white, all-purpose, enriched, bleached',
                                         'Celery': 'Celery, raw',
                                         'Marjoram' : 'Spices, marjoram, dried',
                                         'Cocoa bean, chocolate' : 'Candies, chocolate, dark, NFS (45-59% cacao solids 90%; 60-69% cacao solids 5%; 70-85% cacao solids 5%)',
                                         'Pork, meat' : 'Pork, cured, ham -- water added, whole, boneless, separable lean only, heated, roasted',
                                         'Corn, field, meal' : 'Cornmeal, degermed, enriched, yellow',
                                         'Lemon, juice' : 'Lemon juice, raw',
                                         'Carrot' : 'Carrots, raw',
                                         'Cocoa bean, powder' : 'Cocoa, dry powder, unsweetened',
                                         'Rice, white' : 'Rice, white, long-grain, regular, enriched, cooked',
                                         'Cinnamon' : 'Spices, cinnamon, ground',
                                         'Egg, yolk' : 'Egg, yolk, raw, fresh',
                                         'Pork, meat byproducts': 'Pork, fresh, variety meats and by-products, feet, cooked, simmered',
                                         'Tomato, puree': 'Tomato products, canned, puree, without salt added',
                                         'Coconut, oil': 'Oil, coconut',
                                         #'Herbs, other'
                                         'Egg, white': 'Egg, white, raw, fresh',
                                         'Vinegar': 'Vinegar, distilled',
                                         'Grape, raisin': "Raisins, dark, seedless (Includes foods for USDA's Food Distribution Program)",
                                         'Pepper, nonbell': 'Peppers, jalapeno, raw',
                                         'Wheat, bran': 'Wheat bran, crude',
                                         'Chicken, meat byproducts': 'Chicken, broiler, rotisserie, BBQ, breast, meat only',
                                         'Basil, dried leaves': 'Spices, basil, dried',
                                         'Savory' : 'Spices, sage, ground',
                                         'Corn, field, flour': 'Corn flour, masa, unenriched, white',
                                         'Sugarcane, molasses': 'Molasses',
                                         'Almond': 'Nuts, almonds',
                                         'Beet, sugar, molasses': 'Molasses',
                                         'Chicken, skin': 'Chicken, broilers or fryers, skin only, raw',
                                         'Pepper, black and white': 'Spices, pepper, black',
                                         'Potato, tuber, w/o peel': 'Potatoes, boiled, cooked without skin, flesh, without salt',
                                         'Lettuce, head': 'Lettuce, iceberg (includes crisphead types), raw',
                                         'Ginger, dried': 'Spices, ginger, ground',
                                         'Pea, succulent': 'Peas, green, frozen, cooked, boiled, drained, without salt',
                                         #'Guar, seed'
                                         'Palm, oil': 'Oil, soybean, salad or cooking',
                                         #'Dill, seed': 
                                         'Cucumber': 'Cucumber, with peel, raw',
                                         #'Dillweed':
                                         'Orange, juice': 'Orange juice, chilled, includes from concentrate',
                                         'Seaweed': 'Seaweed, wakame, raw',
                                         'Coconut, dried': 'Nuts, coconut meat, dried (desiccated), sweetened, flaked, packaged',
                                         'Peanut, butter': "Peanut butter, smooth style, with salt (Includes foods for USDA's Food Distribution Program)",
                                         'Blueberry': 'Blueberries, raw',
                                         'Strawberry': 'Strawberries, raw',
                                         'Turkey, meat': 'Turkey, Ground, cooked',
                                         'Bean, snap, succulent': 'Beans, snap, green, frozen, cooked, boiled, drained without salt',
                                         'Coriander, seed': 'Spices, coriander leaf, dried',
                                         'Cilantro, leaves': 'Coriander (cilantro) leaves, raw',
                                         #'Pork, skin'
                                         'Rice, brown': "Rice, brown, long-grain, cooked (Includes foods for USDA's Food Distribution Program)",
                                         'Mushroom': 'Mushrooms, white, raw',
                                         'Turkey, fat': 'Fat, turkey',
                                         'Soybean, seed': 'Soybeans, mature seeds, sprouted, cooked, steamed',
                                         'Ginger': 'Ginger root, raw',
                                         'Turkey, meat byproducts': 'Turkey, all classes, light meat, cooked, roasted',
                                         'Peanut': 'Peanuts, all types, dry-roasted, without salt',
                                         #'Wheat, germ': 
                                         'Turmeric': 'Spices, turmeric, ground',
                                         'Apple, juice': 'Apple juice, canned or bottled, unsweetened, without added ascorbic acid',
                                         'Pepper, nonbell, dried': 'Peppers, jalapeno, raw',
                                         'Pecan' : 'Nuts, pecans',
                                         'Tomato, paste' : "Tomato products, canned, paste, without salt added (Includes foods for USDA's Food Distribution Program)",
                                         'Sheep, meat': 'Lamb, composite of trimmed retail cuts, separable lean and fat, trimmed to 1/4" fat, choice, cooked',
                                         #'Sheep, fat':
                                         'Banana': 'Bananas, raw',
                                         #'Strawberry, juice':
                                         'Grape, juice': 'Grape juice, canned or bottled, unsweetened, without added ascorbic acid',
                                         'Onion, green': 'Onions, young green, tops only',
                                         'Oat, bran': 'Oat bran, raw',
                                         'Cherry': 'Cherries, sweet, raw',
                                         'Cabbage': 'Cabbage, raw',
                                         'Barley, pearled barley': 'Barley, pearled, raw',
                                         'Peach': 'Peaches, yellow, raw',
                                         "Raspberry": 'Raspberries, raw',
                                         'Walnut': 'Nuts, walnuts, english',
                                         'Sheep, meat byproducts': 'Lamb, ground, raw',
                                         'Lime, juice': 'Lime juice, raw',
                                         'Apple, peeled fruit': 'Apples, raw, without skin',
                                         'Sesame, seed': 'Seeds, sesame seeds, whole, dried',
                                         'Corn, sweet': 'Corn, sweet, yellow, frozen, kernels cut off cob, boiled, drained, without salt',
                                         'Pepper, bell': 'Peppers, sweet, green, raw',
                                         'Broccoli': 'Broccoli, raw',
                                         'Bean, lima, succulent': 'Lima beans, immature seeds, frozen, fordhook, cooked, boiled, drained, without salt',
                                         'Grapefruit, juice': 'Grapefruit juice, white, canned or bottled, unsweetened',
                                         'Tangerine, juice': 'Tangerine juice, raw',
                                         'Apple, dried': 'Apples, dried, sulfured, uncooked',
                                         #'Corn, field, bran': 
                                         'Bean, mung, seed': 'Mung beans, mature seeds, sprouted, raw',
                                         'Bean, pinto, seed': 'Beans, pinto, mature seeds, raw',
                                         'Pear, juice': 'Babyfood, juice, pear',
                                         #'Cherry, juice':
                                         'Passionfruit, juice': 'Passion-fruit juice, purple, raw', 
                                         'Apricot, juice': 'Apricot nectar, canned, with added ascorbic acid',
                                         'Pineapple, juice': 'Pineapple juice, canned or bottled, unsweetened, without added ascorbic acid',
                                         'Blackberry': 'Blackberries, raw',
                                         'Grape, wine and sherry': 'Alcoholic beverage, wine, table, all',
                                         'Boysenberry': 'Boysenberries, frozen, unsweetened',
                                         'Turkey, skin': 'Turkey, skin from whole, (light and dark), with added solution, roasted',
                                         #'Raspberry, juice'
                                         'Potato, chips': 'Snacks, potato chips, plain, salted',
                                         'Parsley, dried leaves': 'Parsley, fresh',
                                         'Cranberry, juice': 'Cranberry juice, unsweetened',
                                         #'Coffee, roasted bean':
                                         'Water chestnut': 'Waterchestnuts, chinese, (matai), raw',
                                         'Mango, juice': 'Mango nectar, canned',
                                         'Fish-saltwater finfish, other': 'Fish, tuna, light, canned in water, drained solids',
                                         #'Barley, bran':
                                         'Date': 'Dates, deglet noor',
                                         'Rye, grain': 'Rye flour, medium',
                                         'Bean, kidney, seed': 'Beans, kidney, red, mature seeds, raw',
                                         'Pineapple': 'Pineapple, raw, all varieties',
                                         #'Rice, bran'
                                         #'Pepper, bell, dried'
                                         'Rutabaga': 'Rutabagas, raw',
                                         'Olive': 'Olives, ripe, canned (small-extra large)',
                                         #'Oat, flour':
                                         'Coffee, instant': 'Beverages, coffee, instant, regular, powder',
                                         'Corn, pop': 'Snacks, popcorn, air-popped',
                                         'Maple, sugar': 'Syrups, maple',
                                         #'Orange, peel': 
                                         'Chickpea, seed': 'Chickpeas (garbanzo beans, bengal gram), mature seeds, raw',
                                         'Sunflower, seed': 'Seeds, sunflower seed kernels, dry roasted, without salt',
                                         'Potato, dry (granules/ flakes)': 'Potato flour',
                                         'Pea, edible podded, succulent': 'Beans, snap, green, raw',
                                         'Fish-shellfish, crustacean': 'Crustaceans, crab, blue, cooked, moist heat',
                                         'Bamboo, shoots': 'Bamboo shoots, raw',
                                         #'Citron':
                                         'Plum, prune, juice': 'Prune juice, canned',
                                         'Almond, oil': 'Oil, almond',
                                         'Bean, navy, seed': 'Beans, white, mature seeds, cooked, boiled, without salt',
                                         'Pea, dry': 'Peas, split, mature seeds, cooked, boiled, without salt',
                                         'Buckwheat': 'Buckwheat flour, whole-groat',
                                         'Cranberry, dried': "Cranberries, dried, sweetened (Includes foods for USDA's Food Distribution Program)",
                                         'Plum, prune, dried': 'Plums, dried (prunes), uncooked',
                                         'Parsley, leaves': 'Parsley, fresh',
                                         'Fish-shellfish, mollusc': 'Mollusks, clam, mixed species, raw',
                                         'Lemon': 'Lemons, raw, without peel',
                                         'Pear, dried': 'Pears, dried, sulfured, uncooked',
                                         'Apple, sauce': "Applesauce, canned, unsweetened, without added ascorbic acid (Includes foods for USDA's Food Distribution Program)",
                                         'Asparagus': 'Asparagus, raw',
                                         'Carob': 'Candies, carob, unsweetened',
                                         'Sweet potato' : "Sweet potato, raw, unprepared (Includes foods for USDA's Food Distribution Program)",
                                         'Tamarind': 'Tamarinds, raw',
                                         'Bean, great northern, seed': 'Beans, great northern, mature seeds, canned',
                                         'Potato, tuber, w/peel': 'Potatoes, baked, flesh and skin, without salt',
                                         'Soybean, soy milk': 'Soymilk, original and vanilla, with added calcium, vitamins A and D',
                                         'Rhubarb': 'Rhubarb, raw',
                                         'Fig, dried': 'Figs, dried, uncooked',
                                         'Tomato, juice': 'Tomato juice, canned, with salt added',
                                         'Hazelnut': 'Nuts, hazelnuts or filberts',
                                         'Cabbage, Chinese, napa': 'Cabbage, savoy, raw',
                                         'Cabbage, Chinese, mustard': 'Cabbage, chinese (pak-choi), raw',
                                         'Cabbage, Chinese, bok choy': 'Cabbage, chinese (pak-choi), raw',
                                         'Pear': 'Pears, raw',
                                         'Grape': 'Grapes, red or green (European type, such as Thompson seedless), raw',
                                         'Flax seed, oil': 'Oil, flaxseed, cold pressed',
                                         #'Triticale, flour': 
                                         #'Chicory, roots': ,
                                         'Flax, seed': 'Seeds, flaxseed',
                                         'Shallot, bulb': 'Onions, raw',
                                         'Horseradish': 'Horseradish, prepared',
                                         'Apricot': 'Apricots, raw',
                                         'Banana, dried': 'Snacks, banana chips',
                                         'Grapefruit': 'Grapefruit, raw, pink and red, all areas',
                                         'Lemon, peel': 'Lemon peel, raw',
                                         'Fish-saltwater finfish, tuna': 'Fish, tuna, fresh, yellowfin, raw',
                                         'Pork, liver': 'Braunschweiger (a liver sausage), pork',
                                         'Plum': 'Plums, raw',
                                         'Carrot, juice': 'Carrot juice, canned',
                                         'Beet, garden, roots': 'Beets, raw',
                                         'Spinach': 'Spinach, raw',
                                         #'Celery, juice': ,
                                         'Watercress': 'Watercress, raw',
                                         'Maple syrup': 'Syrups, maple',
                                         'Peach, dried': 'Peaches, dried, sulfured, uncooked',
                                         'Chive, fresh leaves': 'Chives, raw',
                                         'Squash, summer': 'Squash, summer, crookneck and straightneck, raw',
                                         'Fish-freshwater finfish': 'Fish, tuna, fresh, yellowfin, raw',
                                         'Okra': 'Okra, raw',
                                         'Poultry, other, fat': 'Fat, chicken',
                                         'Poultry, other, meat': 'Turkey, Ground, cooked',
                                         'Parsnip': 'Parsnips, cooked, boiled, drained, without salt',
                                         'Dasheen, corm': 'Taro, raw',
                                         'Cauliflower': 'Cauliflower, raw',
                                         'Tomatillo': 'Tomatillos, raw',
                                         'Milk, nonfat solids-baby food/infant formula': 'Infant formula, ABBOTT NUTRITION, SIMILAC, ALIMENTUM, ADVANCE, ready-to-feed, with ARA and DHA',
                                         'Pea, succulent-babyfood': 'Babyfood, peas, dices, toddler',
                                         #'Corn, sweet-babyfood': 
                                         #'Onion, bulb, dried-babyfood': ,
                                         'Carrot-babyfood': 'Babyfood, carrots, toddler',
                                         #'Broccoli-babyfood': ,
                                         #'Bean, snap, succulent-babyfood': ,
                                         #'Tomato, puree-babyfood': ,
                                         'Rice, flour-babyfood': 'Rice flour, white, unenriched',
                                         'Milk, water-babyfood/infant formula': 'Infant formula, ABBOTT NUTRITION, SIMILAC, ALIMENTUM, ADVANCE, ready-to-feed, with ARA and DHA',
                                         'Milk, fat-baby food/infant formula': 'Infant formula, ABBOTT NUTRITION, SIMILAC, ALIMENTUM, ADVANCE, ready-to-feed, with ARA and DHA',
                                         'Soybean, oil-babyfood': 'Infant formula, MEAD JOHNSON, ENFAMIL, ENFAGROW, Soy, Toddler, LIPIL, powder',
                                         'Pumpkin': 'Pumpkin, raw',
                                         'Leek': 'Leeks, (bulb and lower leaf-portion), raw',
                                         'Wild rice': 'Wild rice, raw',
                                         'Arrowroot, flour': 'Tapioca, pearl, dry',
                                         'Bean, black, seed': 'Beans, black, mature seeds, raw'}, inplace=True)

fcid_ingred = fcid_ingred[fcid_ingred["FCID Description"].str.contains('Spices, other|Citron|Orange, peel|Oat, flour|Pepper, bell, dried|Rice, bran|Barley, bran|Coffee, roasted bean|Raspberry, juice|Cherry, juice|Corn, field, bran|Strawberry, juice|Sheep, fat|Wheat, germ|Pork, skin|Dillweed|Dill, seed|Guar, seed|Herbs, other|Triticale, flour|Chicory, roots|Celery, juice|Corn, sweet-babyfood|Onion, bulb, dried-babyfood|Broccoli-babyfood|Bean, snap, succulent-babyfood|Tomato, puree-babyfood')==False]
fcid_ingred.to_csv('../data/03b/fcid_ingredients_0625.csv', index=None)

##PART 3:

fcid = pd.read_csv('../data/03b/fcid_ingredients_0625.csv', usecols=['Main.food.description', 'Ingredient code', 'FCID Description', 'Commodity Weight'])
fndds_all = pd.read_csv('../data/03/manually_curated/fndds_16_18_all_added_codes_100924.csv')
fcid.rename(columns={'Ingredient code':'ingred_code'}, inplace=True)

to_keep = fndds_all[~fndds_all['ingred_code'].isin(fcid['ingred_code'])]
to_replace = fndds_all[fndds_all['ingred_code'].isin(fcid['ingred_code'])]

replaced = to_replace.merge(fcid, how='left', on='ingred_code')
replaced = replaced[['parent_foodcode', 'parent_desc', 'ingred_code', 'FCID Description', 'Commodity Weight']]
#replaced.loc[-1] = [83208000, 'Coleslaw dressing, light', 42230, 'Salad Dressing, coleslaw, reduced fat', 100] # code missing from data added in
replaced.rename(columns={'FCID Description':'ingred_desc', 'Commodity Weight':'ingred_wt'}, inplace=True)
replaced['ingred_wt'] = replaced.groupby(['parent_desc', 'ingred_code', 'ingred_desc'])['ingred_wt'].transform('sum').copy()
replaced = replaced.drop_duplicates(subset=['parent_desc', 'ingred_desc'])
replaced = replaced.merge(fndds_all, on=['parent_desc', 'ingred_code'], how='left')
replaced['ingred_wt'] = replaced['ingred_wt_x'] * (replaced['ingred_wt_y'] / 100)
replaced = replaced[['parent_foodcode_x', 'parent_desc', 'ingred_code', 'ingred_desc_x', 'ingred_wt']]
replaced = replaced.rename(columns={'parent_foodcode_x': 'parent_foodcode', 'ingred_desc_x': 'ingred_desc'})

fcid_desc = fcid[['FCID Description']].drop_duplicates(subset='FCID Description')
fcid_desc.rename(columns={'FCID Description':'ingred_desc'},inplace=True)

fcid_desc = fcid_desc.merge(fndds_all, on='ingred_desc', how='left').drop_duplicates(subset='ingred_desc')
fcid_desc = fcid_desc[['ingred_desc', 'ingred_code']]

replaced_codes = replaced.merge(fcid_desc, on='ingred_desc', how='left')
replaced_codes.drop(columns=['ingred_code_x'],inplace=True)
replaced_codes.rename(columns={'ingred_code_y':'ingred_code'},inplace=True)

fndds_repalced = pd.concat([to_keep, replaced_codes])
fndds_repalced['ingred_wt'] = fndds_repalced.groupby(['parent_desc', 'ingred_desc'])['ingred_wt'].transform('sum').copy()
fndds_repalced = fndds_repalced.drop_duplicates(subset=['parent_desc', 'ingred_desc'])

# harmonize descriptions that describe the same ingredient, but have different text
fndds_repalced['ingred_desc'].replace({"Cheese, cheddar (Includes foods for USDA's Food Distribution Program)": 'Cheese, cheddar',
                                         'Cheese, pasteurized process, American, vitamin D fortified': 'Cheese, pasteurized process, American, fortified with vitamin D',
                                         'Yogurt, plain, low fat, 12 grams protein per 8 ounce': 'Yogurt, plain, low fat',
                                         'Yogurt, plain, skim milk, 13 grams protein per 8 ounce': 'Yogurt, plain, skim milk',
                                         "Cheese, cheddar, reduced fat (Includes foods for USDA's Food Distribution Program)": 'Cheese, cheddar, reduced fat',
                                         'Salt, table': 'Salt, table, iodized',
                                         'Chicken, broiler, rotisserie, BBQ, breast meat only': 'Chicken, broiler, rotisserie, BBQ, breast, meat only',
                                         'Ground turkey, cooked': 'Turkey, Ground, cooked',
                                         'Sausage, Italian, pork, mild, cooked, pan-fried': 'Sausage, Italian, pork, cooked',
                                         "Apples, raw, with skin (Includes foods for USDA's Food Distribution Program)": 'Apples, raw, with skin',
                                         "Applesauce, canned, unsweetened, without added ascorbic acid (Includes foods for USDA's Food Distribution Program)": 'Applesauce, canned, unsweetened, without added ascorbic acid (includes USDA commodity)',
                                         'Applesauce, canned, sweetened, without salt (includes USDA commodity)': 'Applesauce, canned, sweetened, without salt',
                                         "Blueberries, frozen, unsweetened (Includes foods for USDA's Food Distribution Program)": 'Blueberries, frozen, unsweetened',
                                         'Cherries, sour, red, canned, water pack, solids and liquids (includes USDA commodity red tart cherries, canned)': 'Cherries, sour, red, canned, water pack, solids and liquids',
                                         "Cranberries, dried, sweetened (Includes foods for USDA's Food Distribution Program)": 'Cranberries, dried, sweetened',
                                         "Orange juice, raw (Includes foods for USDA's Food Distribution Program)": 'Orange juice, raw',
                                         'Plantains, raw': 'Plantains, yellow, raw',
                                         "Raisins, dark, seedless (Includes foods for USDA's Food Distribution Program)": 'Raisins, seedless',
                                         "Strawberries, frozen, unsweetened (Includes foods for USDA's Food Distribution Program)":'Strawberries, frozen, unsweetened',
                                         "Tomato products, canned, paste, without salt added (Includes foods for USDA's Food Distribution Program)":'Tomato products, canned, paste, without salt added',
                                         "Corn, sweet, yellow, canned, no salt added, solids and liquids (Includes foods for USDA's Food Distribution Program)":'Corn, sweet, yellow, canned, no salt added, solids and liquids',
                                         'Beverages, water, tap, drinking':'Water, tap, drinking',
                                         'Beverages, water, tap, municipal':'Water, tap, municipal',
                                         'Crustaceans, shrimp, mixed species, cooked, moist heat (may contain additives to retain moisture)':'Crustaceans, shrimp, mixed species, cooked, moist heat (may have been previously frozen)',
                                         "Beans, pinto, mature seeds, raw (Includes foods for USDA's Food Distribution Program)":'Beans, pinto, mature seeds, raw',
                                         'Refried beans, canned, traditional style (includes USDA commodity)':'Refried beans, canned, traditional style',
                                         'Beans, kidney, red, mature seeds, canned, solids and liquid, low sodium':'Beans, mature, red kidney, canned, solids and liquid, low sodium',
                                         'Lamb, domestic, shoulder, whole (arm and blade), separable lean and fat, trimmed to 1/4" fat, choice, cooked, roasted':'Lamb, shoulder, whole (arm and blade), separable lean and fat, trimmed to 1/4" fat, choice, cooked, roasted',
                                         "Rice, brown, long-grain, raw (Includes foods for USDA's Food Distribution Program)":'Rice, brown, long-grain, raw',
                                         "Rice, brown, long-grain, cooked (Includes foods for USDA's Food Distribution Program)":'Rice, brown, long-grain, cooked'
                                        }, inplace=True)

fndds_repalced.to_csv('../data/03b/fndds_fcid_all.csv',index=None)