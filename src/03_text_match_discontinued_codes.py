# Import packages
import pandas as pd
import string
import re
from polyfuzz.models import TFIDF
from polyfuzz import PolyFuzz
import nltk
nltk.download(['wordnet', 'omw-1.4'])
wn = nltk.WordNetLemmatizer()

#Load data
discont = pd.read_csv('../data/03/wweia_discontinued_foodcodes.csv')
current = pd.read_csv('../data/02/fndds_16_18_all.csv')

punct = string.punctuation[0:11] + string.punctuation[13:] # remove '-' from the list of punctuation. This is needed for the text cleaner in the following cell

stopwords = ['','and', 'to', 'not', 'no',  'bkdfrd', 'ppd', 'pkgddeli', 'pkgd', 'xtra', 'oz', 'in', 'with', 'or', 'only', 'cooking', 'as', 'food', 'distribution', 'form', 'w', 'wo', 'ns', 'nfs', 'incl']

def clean_text(text):
    text = "".join([word for word in text if word not in punct])
    tokens = re.split('[-\W+]', text)
    text = [word for word in tokens if word not in stopwords]
    text = [wn.lemmatize(word) for word in tokens if word not in stopwords]
    return ' '.join(text)

discont['DRXFCLD_clean'] = discont['DRXFCLD'].apply(lambda x: clean_text(x.lower()))
current['parent_desc_clean'] = current['parent_desc'].apply(lambda x: clean_text(x.lower()))

discont_list = discont['DRXFCLD_clean'].to_list()
current_list = current['parent_desc_clean'].to_list()

tfidf = TFIDF(n_gram_range=(3, 3))
model = PolyFuzz(tfidf).match(discont_list, current_list)

match_str = model.get_matches()
match_str.rename(columns={'From':'DRXFCLD_clean', 'To':'parent_desc_clean'},inplace=True)
fndds_matched = match_str.merge(discont, on='DRXFCLD_clean', how='left')
fndds_matched_ = fndds_matched.merge(current, on='parent_desc_clean', how='left').drop_duplicates(subset='DRXFDCD')

fndds_matched_[['DRXFDCD', 'DRXFCLD', 'parent_foodcode', 'parent_desc', 'Similarity']].sort_values('Similarity', ascending=False).to_csv('../data/03/string_match.csv', index=None)
