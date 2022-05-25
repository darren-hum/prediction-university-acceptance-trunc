from datetime import datetime

import pandas as pd
import numpy as np
from sklearn.preprocessing import OrdinalEncoder, StandardScaler
from math import sqrt
import scipy.stats as spstats
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import train_test_split 

df = pd.read_csv('processed_data.csv')

# Boxcox 
age = np.array(df['age'])
l, opt_lambda = spstats.boxcox(age)
df['age_lambda_opt'] = spstats.boxcox((1+df['age']), lmbda=opt_lambda)
df.drop('age', axis=1, inplace=True)

#### Encoding #### -------------------------------------

#ordinalencode education level
edu = [["'O' Level"], ['A lvl/High School'], ['Pre-diploma'], ['Diploma'], ['Undergrad'], ['Postgrad']]
encoder = OrdinalEncoder()
encoder.fit(edu)
df['edu_level_grp'] = encoder.fit_transform(np.array(df['qualification_lvl']).reshape(-1,1))
df.drop('qualification_lvl', axis=1, inplace=True)

# Get Dummies
df_enc = pd.get_dummies(df, drop_first = True)

#singaporean nationality highly correlates with citizenship = S which makes sense; so drop
df_enc.drop('appl_first_nationality_SGP', axis=1, inplace=True)

#### Train-Split #### ------------------------------------
X = df_enc.drop('enrolled', axis=1)
y = df_enc['enrolled']

X_train, X_test, y_train, y_test = train_test_split(
                                                    X, y, 
                                                    random_state=24, 
                                                    stratify=y)

#25% target. Use SMOTE to resample
sm = SMOTE(random_state=24)
X_train, y_train = sm.fit_resample(X_train,y_train)

#### Scale data #### ---------------------------------------
standard_scale = StandardScaler()
column_names = X_train.columns
X_train = standard_scale.fit_transform(X_train)
X_train = pd.DataFrame(X_train, columns=column_names)

X_test = standard_scale.fit_transform(X_test)
X_test = pd.DataFrame(X_test, columns=column_names)


#### Save Data #### -----------------------------------------
X_test.to_csv('X_test.csv', index=None)
X_train.to_csv('X_train.csv', index=None)

y_test.to_csv('y_test.csv', index=None)
y_train.to_csv('y_train.csv', index=None)


now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Completed at", current_time)











