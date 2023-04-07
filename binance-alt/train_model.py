from build_dataset import BinanceDataset
from sklearn.preprocessing import StandardScaler
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import matplotlib.pyplot as plt
import numpy as np
from sklearn.utils.class_weight import compute_class_weight
from sklearn.model_selection import GridSearchCV

start_date = '2023-03-12'
end_date = '2023-03-12'
symbol = 'MAGICUSDT'

# Retrieve the data from the build dataset class
bds = BinanceDataset(start_date=start_date, end_date=end_date, symbol=symbol)
bds.build()
bds.save_dataset()
data = bds.dataset
print(data.target_variable.value_counts())


# Normalise the data
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


# Train the model
def train_model(X, y, show_plot=False):
    # Split the data into a training set and a testing set
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


    # Using compute_class_weight function
    raw_class_weights = compute_class_weight(class_weight='balanced', classes= np.unique(y_train), y= y_train)
    class_weights = dict(enumerate(raw_class_weights))
    
    print(class_weights)

    param_grid = {
        'n_estimators': [50],
        'max_depth': [None],
        'min_samples_split': [2],
        'min_samples_leaf': [1],
        'max_features': ['sqrt']
    }

    clf = RandomForestClassifier(random_state=42, class_weight=class_weights)
    grid_search = GridSearchCV(estimator=clf, param_grid=param_grid, cv=5, scoring='f1_macro')
    grid_search.fit(X_train, y_train)
    rf_clf = grid_search.best_estimator_

    # Make predictions using the testing set
    y_pred = rf_clf.predict(X_test)

    # Evaluate the classifier performance
    print("Confusion Matrix:")
    print(confusion_matrix(y_test, y_pred))


    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    print("\nAccuracy Score:")
    print(accuracy_score(y_test, y_pred))
    importances = pd.Series(data= rf_clf.feature_importances_, index=X.columns)

    if show_plot:
        fig, ax = plt.subplots()
        importances.plot.bar(ax=ax)
        ax.set_title("Feature importances using MDI")
        ax.set_ylabel("Mean decrease in impurity")
        fig.tight_layout()

        plt.show()

    return rf_clf

        
X, y = normalise(data)

# Create and train the Random Forest classifier
model = train_model(X, y)


# Test the model on the next day's data
nextday_bds = BinanceDataset(start_date='2023-03-13', end_date='2023-03-13', symbol=symbol)
nextday_bds.build()
nextday_bds.save_dataset()

nextday_data = nextday_bds.dataset
print(nextday_data.target_variable.value_counts())


X, y = normalise(nextday_data)

y_pred = model.predict(X)

# Evaluate the classifier performance
print("Confusion Matrix:")
print(confusion_matrix(y, y_pred))
print("\nClassification Report:")
print(classification_report(y, y_pred))
print("\nAccuracy Score:")
print(accuracy_score(y, y_pred))