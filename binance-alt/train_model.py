from build_dataset import BinanceDataset
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import TomekLinks
from imblearn.under_sampling import RandomUnderSampler
import pandas as pd
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
# from smote_variants import SMOTE
# from smote_variants_ts import SMOTE_TS
import matplotlib.pyplot as plt
import numpy as np



start_date = '2023-03-12'
end_date = '2023-03-12'
symbol = 'MAGICUSDT'

dataset_file = f'{symbol}-{start_date}-{end_date}.csv'

if dataset_file in os.listdir('data/datasets/'):
    # Retrieve the data from the saved dataset
    print('> Retrieving data from saved dataset')
    data = pd.read_csv(f"data/datasets/{dataset_file}", index_col=0)
else:
    # Retrieve the data from the build dataset class
    print('> Building new dataset')
    bds = BinanceDataset(start_date=start_date, end_date=end_date, symbol=symbol)
    bds.build()
    bds.save_dataset()
    data = bds.dataset



def normalise(data):

    # Normalizes the data
    features = data.drop(columns=['target_variable'])

    scaler = StandardScaler()

    normalized_features = scaler.fit_transform(features)
    normalized_features_df = pd.DataFrame(normalized_features, columns=features.columns)

    # Train the model
    X = normalized_features_df
    y = data['target_variable']

    return X, y


X, y = normalise(data)

# Split the data into a training set and a testing set
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print(X_train)
# Create and train the Random Forest classifier

class_weights = {
    -1: 0.4,
    0: 0.2,
    1: 0.4,
}

rf_clf = RandomForestClassifier(n_estimators=100, random_state=42, class_weight=class_weights)
rf_clf.fit(X_train, y_train)

# Make predictions using the testing set
y_pred = rf_clf.predict(X_test)

# Evaluate the classifier performance
print("Confusion Matrix:")
print(confusion_matrix(y_test, y_pred, normalize='true'))


print("\nClassification Report:")
print(classification_report(y_test, y_pred))
print("\nAccuracy Score:")
print(accuracy_score(y_test, y_pred))
importances = pd.Series(data= rf_clf.feature_importances_, index=X.columns)

fig, ax = plt.subplots()
importances.plot.bar(ax=ax)
ax.set_title("Feature importances using MDI")
ax.set_ylabel("Mean decrease in impurity")
fig.tight_layout()

plt.show()


nextday_bds = BinanceDataset(start_date='2023-03-13', end_date='2023-03-13', symbol=symbol)
nextday_bds.build()
nextday_bds.save_dataset()

nextday_data = nextday_bds.dataset
