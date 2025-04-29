#!/bin/bash

echo "Initializing step 1: generating ingredient nutrient database"; date
python3 ../src/01_generate_ingredient_nutrient_db.py
echo "Initializing step 2: calculating ingredient weights"; date
python3 ../src/02_calculate_ingredient_weights.py
echo "Initializing step 3: text matching discontinued codes"; date
python3 ../src/03_text_match_discontinued_codes.py
echo "Initializing step 3b: Further ingredientizing with FCID"; date
python3 ../src/03b_fcid_ingredients.py
echo "Initializing step 3c: Adjusting moisture content"; date
python3 ../src/03c_moisture_adjustment.py
echo "Initializing step 4: Generating ingredientized diet recalls"; date
python3 ../src/04_wweia_ingredients_2_day_foodcode.py
echo "Success! Pipeline finished on:"; date
